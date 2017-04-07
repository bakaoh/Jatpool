package com.github.bakaoh.jatpool;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import com.github.bakaoh.jatpool.Jatpool.Config;
import com.github.bakaoh.jatpool.connection.Host;

/**
 *
 * @author taitt
 * @param <Iface>
 */
public class JatpoolBuilder<Iface> {

    private final Class<Iface> iface;
    private Host host;
    private final Config config = new Config();
    private final Class<TServiceClientFactory<? extends TServiceClient>> factory;

    public static <Iface> JatpoolBuilder<Iface> of(Class<Iface> ifaceCls) {
        return new JatpoolBuilder<Iface>(ifaceCls);
    }

    private JatpoolBuilder(Class<Iface> iface) {
        this.iface = iface;
        this.factory = getFactoryClass();
    }

    private Class<TServiceClientFactory<? extends TServiceClient>> getFactoryClass() {
        Class<?>[] classes = iface.getEnclosingClass().getClasses();
        for (Class cls : classes) {
            if ("Client".equals(cls.getSimpleName())) {
                Class[] clientClasses = cls.getClasses();
                for (Class cls1 : clientClasses) {
                    if ("Factory".equals(cls1.getSimpleName())) {
                        return cls1;
                    }
                }
            }
        }
        String className = iface.getCanonicalName();
        className = className.substring(0, className.length() - 5) + "Client.Factory";
        throw new IllegalArgumentException("Factory not found " + className);
    }

    public JatpoolBuilder<Iface> host(String host, int port) {
        this.host = new Host(host, port);
        return this;
    }

    public JatpoolBuilder<Iface> host(Host host) {
        this.host = host;
        return this;
    }

    public JatpoolBuilder<Iface> framed(boolean isFramed) {
        this.config.isFramed = isFramed;
        return this;
    }

    public JatpoolBuilder<Iface> compacted(boolean isCompacted) {
        this.config.isCompacted = isCompacted;
        return this;
    }

    public Config getConfig() {
        return config;
    }

    public Jatpool<Iface> build() {
        return new Jatpool<Iface>(iface, factory, host, config);
    }
}
