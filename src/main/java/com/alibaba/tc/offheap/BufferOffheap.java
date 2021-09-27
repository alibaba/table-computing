package com.alibaba.tc.offheap;

import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.tc.SystemProperty.manualRelease;
import static com.alibaba.tc.offheap.InternalUnsafe.alloc;
import static com.alibaba.tc.offheap.InternalUnsafe.free;
import static com.alibaba.tc.offheap.InternalUnsafe.setMemory;
import static java.lang.String.format;

public class BufferOffheap extends com.alibaba.tc.offheap.AbstractReferenceCounted {
    private static final Logger logger = LoggerFactory.getLogger(BufferOffheap.class);
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BufferOffheap.class).instanceSize();
    private static final AtomicLong bufferSize = new AtomicLong(0L);

    protected long addr;
    protected final long size;

    static long bufferOffheapSize() {
        return bufferSize.get();
    }

    public BufferOffheap(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }

        this.size = size;
        addr = alloc(size);
        if (addr <= 0) {
            throw new IllegalStateException(format("addr: %d", addr));
        }
        bufferSize.addAndGet(Long.BYTES + size);
    }

    public void init() {
        setMemory(addr, size, (byte) 0);
    }

    public void init0(long offset) {
        setMemory(addr + offset, size - offset, (byte) 0);
    }

    public long getAddr() {
        return addr;
    }

    public long getSize() {
        return size;
    }

    @Override
    public boolean release() {
        if (super.release()) {
            safeFree();
            return true;
        }
        return false;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        int n = refCnt();
        if (n > 0 && addr <= 0) {
            throw new IllegalStateException();
        }
        if (0 == n && addr > 0) {
            throw new IllegalStateException();
        }
        if (manualRelease && n != 0) {
            logger.warn("memory leak risk, refCnt: {}", n);
        }
        safeFree();
    }

    private synchronized void safeFree() {
        if (0 != addr) {
            bufferSize.addAndGet(-(Long.BYTES + size));
            free(addr);
            addr = 0;
        }
    }
}