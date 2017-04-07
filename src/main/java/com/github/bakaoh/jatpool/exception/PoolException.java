package com.github.bakaoh.jatpool.exception;

import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import com.github.bakaoh.jatpool.connection.Host;

public class PoolException extends TException {

    private static final long serialVersionUID = -1L;
    private Host host = Host.NO_HOST;
    private long latency = 0;
    private long latencyWithPool = 0;
    private int attemptCount = 0;

    public PoolException(String message) {
        super(message);
    }

    public PoolException(Throwable t) {
        super(t);
    }

    public PoolException(String message, Throwable cause) {
        super(message, cause);
    }

    public PoolException setHost(Host host) {
        this.host = host;
        return this;
    }

    public Host getHost() {
        return this.host;
    }

    public PoolException setLatency(long latency) {
        this.latency = latency;
        return this;
    }

    public long getLatency() {
        return this.latency;
    }

    public long getLatency(TimeUnit units) {
        return units.convert(this.latency, TimeUnit.NANOSECONDS);
    }

    public PoolException setLatencyWithPool(long latency) {
        this.latencyWithPool = latency;
        return this;
    }

    public long getLatencyWithPool() {
        return this.latencyWithPool;
    }

    @Override
    public String getMessage() {
        return getClass().getSimpleName() + ": [host=" + host + ", latency=" + latency + "(" + latencyWithPool
                + "), attempts=" + attemptCount + "] " + super.getMessage();
    }

    public String getOriginalMessage() {
        return super.getMessage();
    }

    public PoolException setAttempt(int attemptCount) {
        this.attemptCount = attemptCount;
        return this;
    }
}
