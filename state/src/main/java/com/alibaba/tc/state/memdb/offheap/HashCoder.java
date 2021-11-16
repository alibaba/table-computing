package com.alibaba.tc.state.memdb.offheap;

public interface HashCoder {
    @StaticMethod
    int hashCode(long addr);
}
