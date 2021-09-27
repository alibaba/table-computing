package com.alibaba.tc.offheap;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public abstract class AbstractReferenceCounted implements ReferenceCounted {
    protected final AtomicInteger refCnt = new AtomicInteger(1);

    @Override
    public int refCnt() {
        return refCnt.get();
    }

    @Override
    public ReferenceCounted retain() {
        refCnt.incrementAndGet();
        return this;
    }

    @Override
    public boolean release() {
        checkRefCnt();
        int n = refCnt.decrementAndGet();
        if (0 == n) {
            return true;
        }
        return false;
    }

    protected void checkRefCnt() {
        int n = refCnt.get();
        if (n <= 0) {
            throw new IllegalStateException(format("refCnf: %d", n));
        }
    }
}
