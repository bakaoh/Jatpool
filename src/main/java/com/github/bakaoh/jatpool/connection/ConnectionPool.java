package com.github.bakaoh.jatpool.connection;

import com.github.bakaoh.jatpool.Jatpool.Config;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.net.SocketTimeoutException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.transport.TTransportException;
import com.github.bakaoh.jatpool.exception.*;

public class ConnectionPool<CL> {

    private final Host host;
    private final TServiceClientFactory<? extends TServiceClient> factory;
    private final Config config;
    private final BlockingQueue<Connection<CL>> availableConnections;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final ScheduledExecutorService executor;
    private final RetryBackoffContext retryContext;
    private final BadHostDetector badHostDetector;
    // counter
    private static final AtomicLong poolIdCounter = new AtomicLong(0);
    private final long id = poolIdCounter.incrementAndGet();
    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final AtomicInteger pendingConnections = new AtomicInteger(0);
    private final AtomicInteger blockedThreads = new AtomicInteger(0);

    public ConnectionPool(Host host,
            TServiceClientFactory<? extends TServiceClient> factory,
            Config config) {
        this.host = host;
        this.factory = factory;
        this.config = config;
        this.availableConnections = new LinkedBlockingQueue<Connection<CL>>();
        this.retryContext = new RetryBackoffContext();
        this.badHostDetector = new BadHostDetector();
        this.executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        });
        this.init();
    }

    private void init() {
        int count = config.initConnectionPerHost;
        for (int i = 0, attemptCount = 0; i < count && attemptCount < 100; i++, attemptCount++) {
            try {
                availableConnections.add(openConnection());
            } catch (PoolException e) {
            } catch (TTransportException e) {
            }
        }
    }

    public Connection<CL> borrow(int timeout) throws PoolException {
        if (isShutdown()) {
            throw new HostDownException("Can't borrow connection. Host is down.").setHost(host);
        }

        Connection<CL> connection = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = availableConnections.poll();
            if (connection != null) {
                return connection;
            }

            boolean isOpenning = tryOpenAsync();

            if (timeout > 0) {
                connection = waitForConnection(isOpenning ? config.connectTimeout : timeout);
                return connection;
            } else {
                throw new PoolTimeoutException("Fast fail waiting for connection from pool")
                        .setHost(host)
                        .setLatency(System.currentTimeMillis() - startTime);
            }
        } finally {

        }
    }

    private Connection<CL> waitForConnection(int timeout) throws PoolException {
        Connection<CL> connection = null;
        long startTime = System.currentTimeMillis();
        try {
            if (blockedThreads.incrementAndGet() <= config.maxBlockThreadsPerHost) {
                connection = availableConnections.poll(timeout, TimeUnit.MILLISECONDS);
                if (connection != null) {
                    return connection;
                }
            } else {
                throw new PoolTimeoutException("Too many clients blocked on this pool " + blockedThreads.get())
                        .setHost(host);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            blockedThreads.decrementAndGet();
        }

        throw new PoolTimeoutException("Timed out waiting for connection").setHost(host).setLatency(
                System.currentTimeMillis() - startTime);
    }

    public boolean returnConnection(Connection<CL> connection, TException ce) {
        if (ce != null) {
            if (ce.getCause() instanceof SocketTimeoutException) {
                if (badHostDetector.addTimeoutSample()) {
                    internalCloseConnection(connection);
                    retryContext.suspend();
                    markAsDown(ce);
                    return true;
                }
            }

            if (ce instanceof TTransportException) {
                internalCloseConnection(connection);
                markAsDown(ce);
                return true;
            }
        }

        if (activeCount.get() <= config.maxConnectionPerHost) {
            availableConnections.add(connection);
            if (isShutdown()) {
                discardIdleConnections();
                return true;
            }
        } else {
            internalCloseConnection(connection);
            return true;
        }

        return false;
    }

    public boolean closeConnection(Connection<CL> connection) {
        internalCloseConnection(connection);
        return true;
    }

    private void internalCloseConnection(Connection<CL> connection) {
        connection.close();
        activeCount.decrementAndGet();
    }

    public void markAsDown(Exception reason) {
        discardIdleConnections();
        if (isShutdown.compareAndSet(false, true)) {
            executor.schedule(new Runnable() {
                public void run() {
                    Thread.currentThread().setName("RetryService : " + host.toString());
                    if (reconnect()) {
                        retryContext.success();
                        isShutdown.set(false);
                    } else {
                        executor.schedule(this, retryContext.getNextDelay(), TimeUnit.MILLISECONDS);
                    }
                }
            }, retryContext.getNextDelay(), TimeUnit.MILLISECONDS);
        }
    }

    private boolean reconnect() {
        Connection<CL> connection = null;
        try {
            activeCount.incrementAndGet();
            connection = new Connection<CL>(host, config, factory);
            availableConnections.add(connection);
            return true;
        } catch (Exception e) {
            activeCount.decrementAndGet();
        }
        return false;
    }

    public void shutdown() {
        isShutdown.set(true);
        executor.shutdown();
        markAsDown(null);
    }

    public Connection<CL> openConnection() throws PoolException, TTransportException {
        if (isShutdown()) {
            throw new HostDownException("Can't open new connection. Host is down.").setHost(host);
        }

        Connection<CL> connection;
        if (activeCount.incrementAndGet() <= config.maxConnectionPerHost) {
            try {
                connection = new Connection<CL>(host, config, factory);
            } catch (TTransportException e) {
                connection = null;
                markAsDown(e);
                activeCount.decrementAndGet();
                throw e;
            }
            if (isShutdown()) {
                connection.close();
                discardIdleConnections();
                activeCount.decrementAndGet();
                throw new HostDownException("Host marked down after connection was created.").setHost(host);
            }
            return connection;
        } else {
            activeCount.decrementAndGet();
            throw new PoolException("Pool exhausted").setHost(host);
        }
    }

    private boolean tryOpenAsync() {
        Connection<CL> connection = null;
        try {
            if (activeCount.incrementAndGet() <= config.maxConnectionPerHost) {
                if (pendingConnections.incrementAndGet() > config.maxPendingPerHost) {
                    pendingConnections.decrementAndGet();
                } else {
                    try {
                        connection = new Connection<CL>(host, config, factory);
                        availableConnections.add(connection);
                        if (isShutdown()) {
                            discardIdleConnections();
                        }
                        return true;
                    } catch (TTransportException ex) {
                        return false;
                    } finally {
                        if (connection == null) {
                            pendingConnections.decrementAndGet();
                        }
                    }
                }
            }
        } finally {
            if (connection == null) {
                activeCount.decrementAndGet();
            }
        }
        return false;
    }

    private void discardIdleConnections() {
        List<Connection<CL>> connections = new ArrayList<Connection<CL>>();
        availableConnections.drainTo(connections);
        activeCount.addAndGet(-connections.size());
        for (Connection<CL> connection : connections) {
            connection.close();
        }
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public Host getHost() {
        return host;
    }

    public Config getConfig() {
        return config;
    }

    @Override
    public String toString() {
        int idle = availableConnections.size();
        int open = activeCount.get();
        int pending = pendingConnections.get();
        int blocked = blockedThreads.get();
        return new StringBuilder().append("ConnectionPool[")
                .append("host=").append(host).append("-").append(id)
                .append(",active=").append(!isShutdown())
                .append(",open=").append(open)
                .append(",busy=").append(open - idle - pending)
                .append(",idle=").append(idle)
                .append(",blocked=").append(blocked)
                .append(",pending=").append(pending)
                .append("]").toString();
    }

    private class BadHostDetector {

        private final LinkedBlockingQueue<Long> timeouts = new LinkedBlockingQueue<Long>();

        /**
         * Add a timeout sample and return false if the host should be
         * quarantined
         *
         * @return true to quarantine or false to continue using this host
         */
        public boolean addTimeoutSample() {
            long currentTimeMillis = System.currentTimeMillis();

            timeouts.add(currentTimeMillis);
            if (timeouts.size() > config.badHostTimeoutCount) {
                Long last = timeouts.remove();
                if ((currentTimeMillis - last) < config.badHostTimeoutWindow) {
                    return true;
                }
            }
            return false;
        }
    }

    private class RetryBackoffContext {

        private int c = 1;
        private final AtomicBoolean isSuspended = new AtomicBoolean(false);
        private int attemptCount = 0;
        private long lastReconnectTime = 0;

        /**
         * Return the next backoff delay in the strategy
         *
         * @return
         */
        public long getNextDelay() {
            if (isSuspended.get()) {
                isSuspended.set(false);
                return config.retrySuspendWindow;
            }

            attemptCount++;
            if (attemptCount == 1) {
                if (System.currentTimeMillis() - lastReconnectTime < config.retrySuspendWindow) {
                    return config.retrySuspendWindow;
                }
            }

            c *= 2;
            if (c > config.retryMaxDelaySlice) {
                c = config.retryMaxDelaySlice;
            }

            return (new Random().nextInt(c) + 1) * config.retryDelaySlice;
        }

        public int getAttemptCount() {
            return attemptCount;
        }

        /**
         * Start the reconnect process
         */
        public void begin() {
            this.attemptCount = 0;
            this.c = 1;
        }

        /**
         * Called when a connection was established successfully
         */
        public void success() {
            this.lastReconnectTime = System.currentTimeMillis();
        }

        /**
         * Suspend the host for being bad (i.e. timing out too much)
         */
        public void suspend() {
            this.isSuspended.set(true);
        }
    }
}
