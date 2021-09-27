package com.alibaba.tc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class IpUtil {
    private static final Logger logger = LoggerFactory.getLogger(IpUtil.class);

    private static String ip;

    public static String getIp() {
        if (null != ip) {
            return ip;
        }

        if (System.getProperty("os.name").contains("Mac OS X")) {
            ip = getIpByName("en0");
        } else {
            ip = getIpByName("bond0");
        }

        ip = ip.replace("/", "");
        return ip;
    }

    public static String getIpByName(String name) {
        try {
            Enumeration e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface n = (NetworkInterface) e.nextElement();
                if (name.equals(n.getName())) {
                    Enumeration ee = n.getInetAddresses();
                    while (ee.hasMoreElements()) {
                        InetAddress i = (InetAddress) ee.nextElement();
                        if (i instanceof Inet4Address) {
                            return i.getHostAddress();
                        }
                    }
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        return "unknown";
    }
}
