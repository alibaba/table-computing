package com.alibaba.jstream.sp;

import static java.util.Objects.requireNonNull;

public class Node implements Comparable<Node> {
    private final String host;
    private final int port;

    public Node(String host, int port) {
        this.host = requireNonNull(host);
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int compareTo(Node that) {
        if (this == that) {
            return 0;
        }

        if (this.host.equals(that.host)) {
            return this.port - that.port;
        }

        return this.host.compareTo(that.host);
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another instanceof Node) {
            Node that = (Node) another;
            return host.equals(that.host) && port == that.port;
        }
        return false;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
