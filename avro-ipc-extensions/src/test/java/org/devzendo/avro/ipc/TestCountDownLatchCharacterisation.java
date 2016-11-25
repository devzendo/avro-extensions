package org.devzendo.avro.ipc;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(MockitoJUnitRunner.class)
public class TestCountDownLatchCharacterisation {
    private static final Logger logger = LoggerFactory.getLogger(TestTimeoutDecorator.class);

    @Test
    public void checkCountDownLatchTimeoutAssumptions() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                logger.debug("Starting wait 2s thread");
                try {
                    Thread.sleep(2000);
                    logger.debug("Finished 2s wait, counting down latch");
                    latch.countDown();
                    logger.debug("Latch counted down; wait thread finishing");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        logger.debug("waiting 1s for latch to be counted down");
        latch.await(1000, TimeUnit.MILLISECONDS);
        logger.debug("finished 1s await on latch; latch count is " + latch.getCount());
        assertThat(latch.getCount(), equalTo(1L));
        logger.debug("waiting another 3s for latch to be counted down");
        latch.await(3000, TimeUnit.MILLISECONDS);
        logger.debug("finished 3s await on latch; latch count is " + latch.getCount());
        assertThat(latch.getCount(), equalTo(0L));
    }
}
