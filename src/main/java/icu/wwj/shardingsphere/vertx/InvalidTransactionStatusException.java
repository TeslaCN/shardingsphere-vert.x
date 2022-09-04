package icu.wwj.shardingsphere.vertx;

import io.vertx.core.impl.NoStackTraceThrowable;

public class InvalidTransactionStatusException extends NoStackTraceThrowable {
    
    public InvalidTransactionStatusException(final String message) {
        super(message);
    }
}
