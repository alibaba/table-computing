package com.alibaba.jstream.table;

import java.util.HashMap;
import java.util.Map;

public class As {
    private final Map<String, String> map = new HashMap<>();

    public As as(String src, String dst) {
        map.put(src, dst);
        return this;
    }

    public Map<String, String> build() {
        return map;
    }
}
