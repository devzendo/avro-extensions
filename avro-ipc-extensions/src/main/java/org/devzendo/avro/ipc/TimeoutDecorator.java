package org.devzendo.avro.ipc;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.Requestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Constructs a dynamic proxy of a given interface class (that supports Callbacks), that modifies methods to obtain
 * their result given a timeout value, or to time out if the wait exceeds that time.
 */
public class TimeoutDecorator implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(TimeoutDecorator.class);

    /**
     * Construct a dynamic proxy that will time out methods after a given number of milliseconds.
     * @param timeoutMs the timeout in milliseconds.
     */
    public static <T, C> T decorate(long timeoutMs, Class<T> iface, Class<C> callbackIface, C callbackProxy) throws IOException {
        if (handshakePresent(iface, false)) {
            if (handshakePresent(callbackIface, true)) {
                throw new AvroTypeException("no handshake one-way message defined");
            }
        } else {
            throw new AvroTypeException("no handshake message defined in non-callback interface");
        }

        return (T) Proxy.newProxyInstance
                (iface.getClassLoader(),
                        new Class[] { iface },
                        new TimeoutDecorator(timeoutMs, callbackIface, callbackProxy));
    }

    private static <T> boolean handshakePresent(Class<T> iface, boolean singleCallbackArgumentExpected) {
        for (Method method : iface.getMethods()) {
            if (method.getName().equals("handshake")) {
                final int numParams = method.getParameterTypes().length;
                return (!singleCallbackArgumentExpected && numParams == 0) ||

                        (singleCallbackArgumentExpected && numParams == 1 &&
                                method.getParameterTypes()[0] instanceof Class  &&
                                Callback.class.isAssignableFrom(((Class<?>)method.getParameterTypes()[0])));
            }
        }
        return false;
    }

    private final long timeoutMs;
    private final Class<?> callbackIface;
    private final Object callbackProxy;

    private <T, C> TimeoutDecorator(long timeoutMs, Class<C> callbackIface, C callbackProxy) {
        logger.debug("Decorating callback interface {} to time out after {} ms",
                callbackIface.getName(), timeoutMs);
        this.timeoutMs = timeoutMs;
        this.callbackIface = callbackIface;
        this.callbackProxy = callbackProxy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        logger.debug("Calling method {}", name);
        if (name.equals("hashCode")) {
            return hashCode();
        }
        else if (name.equals("equals")) {
            Object obj = args[0];
            return (proxy == obj) || (obj != null && Proxy.isProxyClass(obj.getClass())
                    && this.equals(Proxy.getInvocationHandler(obj)));
        }
        else if (name.equals("toString")) {
            String protocol = "unknown";
            String remote = "unknown";
            Class<?>[] interfaces = proxy.getClass().getInterfaces();
            if (interfaces.length > 0) {
                try {
                    protocol = Class.forName(interfaces[0].getName()).getSimpleName();
                } catch (ClassNotFoundException e) {
                }

                InvocationHandler handler = Proxy.getInvocationHandler(proxy);
                if (handler instanceof Requestor) {
                    try {
                        remote = ((Requestor) handler).getTransceiver().getRemoteName();
                    } catch (IOException e) {
                    }
                }
            }
            return "Proxy[" + protocol + "," + remote + "]";
        }

        // So this is of (dynamic) type T, the non-callback interface.
        // Needs to create a Callback, and pass it to the correct method of the callback proxy (along with the rest of
        // the args).
        // Then, get the result of the callback, waiting timeoutMs ms. If it times out, throw AvroTimeoutException.

        logger.debug("Performing timeout method invocation...");
        final CallFuture<?> callback = new CallFuture();
        // Append the Callback to the end of of the argument list
        Object[] argsWithCallback = copyArgs(args, callback);
        // Find the appropriate method in the callback interface.
        Class[] parameterTypes = method.getParameterTypes();
        Class[] argClassesWithCallback = Arrays.copyOf(parameterTypes, parameterTypes.length + 1);
        argClassesWithCallback[parameterTypes.length] = Callback.class;
        Method callbackMethod = callbackIface.getMethod(name, argClassesWithCallback);
        logger.debug("invoking {}", callbackMethod);

        try {
            // Call the callback proxy's method
            long start = System.currentTimeMillis();
            callbackMethod.invoke(callbackProxy, argsWithCallback);

            // "waiting is..." - Heinlein
            Object returnValue = callback.get(timeoutMs, TimeUnit.MILLISECONDS);
            long stop = System.currentTimeMillis();
            logger.debug("method returned '{}' within {} ms - actual wait {} ms", returnValue, timeoutMs, stop - start);
            return returnValue;

        } catch (TimeoutException te) {
            throw new AvroTimeoutException("method call '" + callbackMethod.getName() + "' timed out after " + timeoutMs + "ms", te);

        } catch (Exception e) {
            // Check if this is a declared Exception:
            for (Class<?> exceptionClass : method.getExceptionTypes()) {
                if (exceptionClass.isAssignableFrom(e.getClass())) {
                    throw e;
                }
            }

            // Next, check for RuntimeExceptions:
            if (e instanceof RuntimeException) {
                throw e;
            }

            // Not an expected Exception, so wrap it in AvroRemoteException:
            throw new AvroRemoteException(e);
        }
    }

    private Object[] copyArgs(Object[] args, CallFuture<?> callback) {
        if (args == null) {
            return new Object[] { callback };
        }
        Object[] argsWithCallback = Arrays.copyOf(args, args.length + 1);
        argsWithCallback[args.length] = callback;
        return argsWithCallback;
    }
}
