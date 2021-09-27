package com.alibaba.tc.table;

import com.alibaba.tc.offheap.ReferenceCounted;

public interface ColumnInterface<T extends Comparable> extends Serializable, ReferenceCounted {
    long size();
    void add(T comparable);
    T get(int row);
}
