package com.github.bakaoh.jatpool.exception;

public class PoolTimeoutException extends PoolException {
   
    private static final long serialVersionUID = -8579946319118318717L;

    public PoolTimeoutException(String message) {
        super(message);
    }

    public PoolTimeoutException(Throwable t) {
        super(t);
    }

    public PoolTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
