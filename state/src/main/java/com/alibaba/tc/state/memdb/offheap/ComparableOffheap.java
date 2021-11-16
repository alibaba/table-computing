package com.alibaba.tc.state.memdb.offheap;

public interface ComparableOffheap<T> extends Comparable<T> {
    int compareTo(long addr);
}
