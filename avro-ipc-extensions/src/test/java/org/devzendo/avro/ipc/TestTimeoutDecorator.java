package org.devzendo.avro.ipc;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(MockitoJUnitRunner.class)
public class TestTimeoutDecorator {

    private static final Logger logger = LoggerFactory.getLogger(TestTimeoutDecorator.class);

    public static final int PORT = 65111;
    public static final long METHOD_TIMEOUT_MS = 500L;
    private NettyTransceiver client;

    private Server server;

    public static class TestProtoImpl implements TestProto {
        private Pattern waitPattern = Pattern.compile("^wait (\\d+)$");

        @Override
        public void handshake() {
            logger.info("Received a handshake =============================================================");
        }

        public Utf8 send(Message message) {
            String body = message.getBody().toString();
            logger.info("Received a message to send =============================== [" + body + "] ===============================");
            if (body.equals("throw")) {
                throw new RuntimeException("boom!");
            } else {
                Matcher matcher = waitPattern.matcher(body);
                if (matcher.matches()) {
                    long ms = Long.parseLong(matcher.group(1));
                    logger.info("Waiting for " + ms + " ms");
                    try {
                        Thread.sleep(ms);
                    } catch (InterruptedException e) {
                        logger.warn("send's fake sleep was interrupted, how rude!", e);
                    }
                    logger.info("Finished waiting");
                }
            }
            return new Utf8("Response from server with body " + body);
        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void startServer() throws IOException {
        server = new NettyServer(new SpecificResponder(TestProto.class, new TestProtoImpl()), new InetSocketAddress(PORT));
        client = new NettyTransceiver(new InetSocketAddress(PORT), 2000L);
    }

    @After
    public void stopServer()
    {
        client.close();
        server.close();
    }

    @Test
    public void aNormalCallThatReturns() throws IOException {
        TestProto proxy = (TestProto) SpecificRequestor.getClient(TestProto.class, client);
        Message message = new Message();
        message.setBody(new Utf8("hello"));
        assertThat(proxy.send(message).toString(), equalTo("Response from server with body hello"));
    }

    @Test
    public void aNormalCallThatTakesALongTimeDoesNotTimeout() throws IOException {
        TestProto proxy = (TestProto) SpecificRequestor.getClient(TestProto.class, client);
        Message message = new Message();
        message.setBody(new Utf8("wait 1000"));
        long start = System.currentTimeMillis();
        String returned = proxy.send(message).toString();
        long stop = System.currentTimeMillis();
        assertThat(returned, equalTo("Response from server with body wait 1000"));
        long duration = stop - start;
        logger.info("Round-trip was " + duration + " ms");
        assertThat(duration, greaterThanOrEqualTo(1000L));
        assertThat(duration, lessThan(2000L)); // it really shouldn't be this long though
    }

    @Test
    public void aNormalInitialHandshakeCallWithATimeoutThatTakesALongTimeTimesout() throws IOException, InterruptedException {
        TestProto proxy = (TestProto) SpecificRequestor.getClient(TestProto.class, client, METHOD_TIMEOUT_MS);
        thrown.expect(AvroRemoteException.class);
        thrown.expectCause(isA(TimeoutException.class));

        Message message = new Message();
        message.setBody(new Utf8("wait 1000"));
        proxy.send(message).toString();

        // don't tear down client / server yet
        logger.info("waiting in test");
        Thread.sleep(2000L);
        logger.info("finished wait in test");
    }

    @Test
    public void serverTerminatingIsDetectedByClient() throws IOException {
        server.close();

        TestProto proxy = (TestProto) SpecificRequestor.getClient(TestProto.class, client);
        Message message = new Message();
        message.setBody(new Utf8("quit"));

        thrown.expect(AvroRemoteException.class);
        thrown.expectMessage("java.io.IOException: Error connecting to 0.0.0.0/0.0.0.0:" + PORT);
        proxy.send(message);
    }

    @Test
    public void serverThrowingIsDetectedByClient() throws IOException {
        TestProto proxy = (TestProto) SpecificRequestor.getClient(TestProto.class, client);
        Message message = new Message();
        message.setBody(new Utf8("throw"));

        thrown.expect(AvroRuntimeException.class);
        thrown.expectMessage("org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.RuntimeException: java.lang.RuntimeException: boom!");
        proxy.send(message);
    }

    @Test
    public void aNormalCallWithACallbackThatTakesALongTimeCanBeTimedOutOnceConnectionIsEstablished() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        TestProto.Callback callbackProxy = SpecificRequestor.getClient(TestProto.Callback.class, client);

        callbackProxy.handshake();

        // ok, now we're connected. Make another call with a callback, and detect a timeout

        thrown.expect(TimeoutException.class);
        thrown.expectMessage(nullValue(String.class)); // it's not ideal, but gives you a clue!

        CallFuture<CharSequence> callFuture = new CallFuture<CharSequence>();
        Message message = new Message();
        message.setBody(new Utf8("wait 4000"));
        long start = System.currentTimeMillis();
        logger.debug("about to call send on the proxy with a callfuture");
        callbackProxy.send(message, callFuture);  // Without the initial connection establishment, THIS ISN'T RETURNING IMMEDIATELY, IT'S BLOCKING FOR 4000 MS
        long stop = System.currentTimeMillis();
        long duration = stop - start;
        logger.info("Call with callfuture round-trip was " + duration + " ms");
        assertThat(duration, lessThan(200L)); // should return straight away, give or take comms round trip transit time (seen: 46ms)
        logger.debug("waiting 2s for response");
        callFuture.await(2000, TimeUnit.MILLISECONDS); // THIS IS WAITING UNTIL THE CALL COMPLETES, SO, 4000 MS
        // never gets here due to TimeoutException
    }

    // Now test the timeout decorator...

    @Test
    public void remoteCallsAreTimedOutIfTheyTakeTooLong() throws IOException, InterruptedException {
        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(65111), 2000L);
        TestProto.Callback proxy = SpecificRequestor.getClient(TestProto.Callback.class, client);
        TestProto decorated = TimeoutDecorator.decorate(500, TestProto.class, TestProto.Callback.class, proxy);

        logger.info("Calling handshake to connect");
        proxy.handshake(); // one-way methods are not present in the TestProto.CallBack interface
        logger.info("Calling handshake to see onewayness");
        proxy.handshake(); // yes this is one-way, but can't test for that.

        thrown.expect(AvroTimeoutException.class);
        thrown.expectCause(isA(TimeoutException.class));
        thrown.expectMessage("method call 'send' timed out after 500ms");

        Message message = new Message();
        message.setBody(new Utf8("wait 2000"));
        decorated.send(message);

        // don't tear down client / server yet
        logger.info("waiting in test");
        Thread.sleep(4000L);
        logger.info("finished wait in test");
    }

    @Test
    public void decoratedProxyMustContainHandshakeMethod() throws IOException {
        thrown.expect(AvroTypeException.class);
        thrown.expectMessage("no handshake message defined in non-callback interface");

        TimeoutDecorator.decorate(500, NoHandshake.class, NoHandshake.Callback.class, null);
    }

    @Test
    public void handshakeMustBeOneWay() throws IOException {
        thrown.expect(AvroTypeException.class);
        thrown.expectMessage("no handshake one-way message defined");

        TimeoutDecorator.decorate(500, HandshakeNotOneWay.class, HandshakeNotOneWay.Callback.class, null);
    }

    @Test
    public void handshakeCorrectDefinition() throws IOException {
        TimeoutDecorator.decorate(500, JustHandshake.class, JustHandshake.Callback.class, null);
        // all is cool
    }
}
