package com.alibaba.tc.offheap;

public interface ReferenceCounted {
    int refCnt();
    ReferenceCounted retain();

    /**
     * Decreases the reference count by {@code 1} and deallocates this object if the reference count reaches at
     * {@code 0}.
     *
     * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
     */
    boolean release();
}
