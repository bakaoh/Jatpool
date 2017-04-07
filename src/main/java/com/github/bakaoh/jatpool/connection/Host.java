package com.github.bakaoh.jatpool.connection;

/**
 *
 * @author taitt
 */
public class Host {

    public static final Host NO_HOST = new Host("0.0.0.0", 0);
    private final String ipAddress;
    private final int port;

    public Host(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public static Host parse(String hostport) {
        try {
            String[] parts = hostport.split(":");
            return new Host(parts[0], Integer.parseInt(parts[1]));
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid host port", ex);
        }
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return ipAddress + ":" + port;
    }
}
