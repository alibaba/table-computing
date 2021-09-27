package com.alibaba.tc;

import com.alibaba.tc.sp.Node;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

public class SystemProperty {
    public static final boolean DEBUG = parseBoolean(System.getProperty("debug"));
    private static Node self = null;
    private static List<Node> all = new ArrayList<>();
    private static int myHash = 0;

    static {
        init();
    }

    public static void init() {
        String strSelf = System.getProperty("self");
        if (null == strSelf) {
            return;
        }

        String strAll = System.getProperty("all");
        String[] strings = strSelf.split(":");
        self = new Node(strings[0], parseInt(strings[1]));

        String[] nodes = strAll.split(",");
        for (int i = 0; i < nodes.length; i++) {
            strings = nodes[i].split(":");
            Node tmp = new Node(strings[0], parseInt(strings[1]));
            if (all.contains(tmp)) {
                throw new IllegalArgumentException("duplicated node, check your -Dall= ");
            }
            all.add(tmp);
        }

        all.sort((o1, o2) -> o1.compareTo(o2));

        myHash = all.indexOf(self);
        if (myHash < 0) {
            throw new IllegalArgumentException("self is not exists in all");
        }
    }

    public static Node getSelf() {
        return self;
    }

    public static String mySign() {
        return null == self ? "" : self.toString();
    }

    public static int getMyHash() {
        return myHash;
    }

    public static int getServerCount() {
        return all.size() == 0 ? 1 : all.size();
    }

    public static Node getNodeByHash(int hash) {
        return all.get(hash);
    }
}
