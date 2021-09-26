package com.alibaba.jstream.table;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentIndex<V> {
    private final Map<List<Comparable>, V> index = new ConcurrentHashMap<>();

    public void put(List<Comparable> key, V v) {
        index.put(key, v);
    }

    public void remove(List<Comparable> key) {
        index.remove(key);
    }

    public V get(List<Comparable> key) {
        return index.get(key);
    }

    public V get(Comparable... key) {
        return index.get(Arrays.asList(key));
    }

    public boolean containsKey(Comparable... key) {
        return index.containsKey(key);
    }
}
