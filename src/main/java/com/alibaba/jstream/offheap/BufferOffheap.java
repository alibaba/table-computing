package com.alibaba.jstream.offheap;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.jstream.offheap.InternalUnsafe.alloc;
import static com.alibaba.jstream.offheap.InternalUnsafe.allocateInstance;
import static com.alibaba.jstream.offheap.InternalUnsafe.free;
import static com.alibaba.jstream.offheap.InternalUnsafe.putInt;
import static com.alibaba.jstream.offheap.InternalUnsafe.putLong;
import static com.alibaba.jstream.offheap.InternalUnsafe.putObject;
import static com.alibaba.jstream.offheap.InternalUnsafe.setMemory;
import static io.airlift.slice.Slices.EMPTY_SLICE;

public class BufferOffheap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BufferOffheap.class).instanceSize();
    private static final AtomicLong bufferSize = new AtomicLong(0L);

    protected final long addr;
    protected final long size;

    static long bufferOffheapSize() {
        return bufferSize.get();
    }

    public static Slice newSlice(int size)
    {
        if (size < 0) {
            throw new IllegalArgumentException();
        }

        if (0 == size) {
            return EMPTY_SLICE;
        }

        Slice slice = (Slice) allocateInstance(Slice.class);
        BufferOffheap bufferOffheap = new BufferOffheap(size);
        putLong(slice, "address", bufferOffheap.addr);
        putInt(slice, "size", size);
        putLong(slice, "retainedSize", size + EMPTY_SLICE.getRetainedSize() + BufferOffheap.INSTANCE_SIZE);
        putObject(slice, "reference", bufferOffheap);

        return slice;
    }

    public BufferOffheap(long size)
    {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }

        this.size = size;
        addr = alloc(size);
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
    protected void finalize() throws Throwable {
        super.finalize();
        if (0L == addr) {
            throw new IllegalStateException();
        }
        bufferSize.addAndGet(-(Long.BYTES + size));
        free(addr);
    }
}