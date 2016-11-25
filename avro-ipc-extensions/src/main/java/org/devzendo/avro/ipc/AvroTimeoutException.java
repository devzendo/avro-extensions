package org.devzendo.avro.ipc;

import org.apache.avro.AvroRuntimeException;

/**
 * Thrown by client proxies decorated by TimeoutDecorator, when the timeout specified has been exceeded.
 */
public class AvroTimeoutException extends AvroRuntimeException {
    public AvroTimeoutException(Throwable cause) {
        super(cause);
    }

    public AvroTimeoutException(String message) {
        super(message);
    }

    public AvroTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
