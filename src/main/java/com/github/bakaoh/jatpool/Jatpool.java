package com.github.bakaoh.jatpool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import com.github.bakaoh.jatpool.connection.Connection;
import com.github.bakaoh.jatpool.connection.ConnectionPool;
import com.github.bakaoh.jatpool.connection.Host;

/**
 *
 * @author taitt
 * @param <I>
 */
public class Jatpool<I> {

    private static final Map<String, Jatpool> INSTANCES = new ConcurrentHashMap<String, Jatpool>();

    public static <I> Jatpool<I> getInstance(Class<I> iface, String hostPort) {
        String key = mapCode(iface, hostPort);
        if (INSTANCES.get(key) == null) {
            INSTANCES.put(key, JatpoolBuilder.of(iface).host(Host.parse(hostPort)).build());
        }
        return INSTANCES.get(key);
    }

    private static String mapCode(Class iface, String hostPort) {
        return iface.getCanonicalName() + "_" + hostPort;
    }

    public static class Config {

        private boolean locked = false;
        public boolean isFramed = true;
        public boolean isCompacted = false;
        public int retryNum = 3;
        public int connectTimeout = 1000;
        public int socketTimeout = 3000;
        public int initConnectionPerHost = 5;
        public int maxConnectionPerHost = 100;
        public int maxBlockThreadsPerHost = 10;
        public int maxPendingPerHost = 100;
        public int maxTimeWaitForConnection = 2000;
        public int badHostTimeoutCount = 3;
        public int badHostTimeoutWindow = 10000;
        public int retryMaxDelaySlice = 10;
        public int retryDelaySlice = 1000;
        public int retrySuspendWindow = 20000;

        public Config() {
        }

        public Config(Config that) {
            this.locked = that.locked;
            this.isFramed = that.isFramed;
            this.isCompacted = that.isCompacted;
            this.retryNum = that.retryNum;
            this.connectTimeout = that.connectTimeout;
            this.socketTimeout = that.socketTimeout;
            this.initConnectionPerHost = that.initConnectionPerHost;
            this.maxConnectionPerHost = that.maxConnectionPerHost;
            this.maxBlockThreadsPerHost = that.maxBlockThreadsPerHost;
            this.maxPendingPerHost = that.maxPendingPerHost;
            this.maxTimeWaitForConnection = that.maxTimeWaitForConnection;
            this.badHostTimeoutCount = that.badHostTimeoutCount;
            this.badHostTimeoutWindow = that.badHostTimeoutWindow;
            this.retryMaxDelaySlice = that.retryMaxDelaySlice;
            this.retryDelaySlice = that.retryDelaySlice;
            this.retrySuspendWindow = that.retrySuspendWindow;
        }
    }

    private final Config config;
    private final ConnectionPool<I> pool;
    private final I client;

    Jatpool(Class<I> iface, Class<TServiceClientFactory<? extends TServiceClient>> factoryCls,
            Host host, Config config) {
        this.config = new Config(config);
        this.config.locked = true;
        this.pool = new ConnectionPool<I>(host, createFactory(factoryCls), this.config);
        this.client = (I) Proxy.newProxyInstance(
                iface.getClassLoader(),
                new Class[]{iface},
                new PoolHandler());
    }

    private TServiceClientFactory<? extends TServiceClient> createFactory(
            Class<TServiceClientFactory<? extends TServiceClient>> factoryCls) {
        try {
            return factoryCls.newInstance();
        } catch (InstantiationException ex) {
            throw new IllegalArgumentException("Factory not instantiable. ");
        } catch (IllegalAccessException ex) {
            throw new IllegalArgumentException("Factory not instantiable. ");
        }
    }

    public I cli() {
        return client;
    }

    public void close() {
        this.pool.shutdown();
    }

    private class PoolHandler implements InvocationHandler {

        public Object invoke(Object proxy, Method method, Object[] args) throws TException {
            TException lastException = null;
            for (int i = 0; i < config.retryNum; i++) {
                Connection conn = null;
                try {
                    conn = pool.borrow(config.maxTimeWaitForConnection);
                    return conn.execute(method, args);
                } catch (TApplicationException ae) {
                    throw ae;
                } catch (TException pe) {
                    lastException = pe;
                } finally {
                    if (conn != null) {
                        pool.returnConnection(conn, lastException);
                    }
                }
            }
            throw lastException;
        }
    }
}
