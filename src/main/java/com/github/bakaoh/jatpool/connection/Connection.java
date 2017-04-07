package com.github.bakaoh.jatpool.connection;

import com.github.bakaoh.jatpool.Jatpool.Config;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author taitt
 */
public class Connection<Iface> {

    private final Iface client;
    private TSocket socket;
    private TTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Connection(Host host, Config config, TServiceClientFactory<? extends TServiceClient> factory)
            throws TTransportException {
        socket = new TSocket(host.getIpAddress(), host.getPort(), config.socketTimeout);
        try {
            socket.getSocket().setTcpNoDelay(true);
            socket.getSocket().setKeepAlive(true);
            socket.getSocket().setSoLinger(false, 0);
        } catch (SocketException e) {
            // the underlying socket implementation doesn’t support these options
        }
        transport = config.isFramed ? new TFramedTransport(socket) : socket;
        transport.open();
        client = (Iface) factory.getClient(config.isCompacted
                ? new TCompactProtocol(transport)
                : new TBinaryProtocol(transport));
    }

    public Object execute(Method method, Object[] args) throws TException {
        try {
            return method.invoke(client, args);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Poor implement. ", ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Poor implement. ", ex);
        } catch (InvocationTargetException ite) {
            Throwable ex = ite.getCause();
            if (ex instanceof TException) {
                throw (TException) ex;
            } else {
                throw new RuntimeException("Poor implement. ", ex);
            }
        }
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                if (transport != null) {
                    transport.flush();
                    transport.close();
                    transport = null;
                }
                if (socket != null) {
                    socket.close();
                    socket = null;
                }
            } catch (Exception e) {
            }
        }
    }
}
