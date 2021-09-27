package com.alibaba.tc.table;

public interface ColumnInterface<T extends Comparable> extends Serializable {
    long size();
    void add(T comparable);
    T get(int row);
}
