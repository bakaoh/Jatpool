package com.github.bakaoh.jatpool.exception;

public class HostDownException extends PoolException {

    private static final long serialVersionUID = 1L;

    public HostDownException(String message) {
        super(message);
    }

    public HostDownException(Throwable t) {
        super(t);
    }

    public HostDownException(String message, Throwable cause) {
        super(message, cause);
    }
}
