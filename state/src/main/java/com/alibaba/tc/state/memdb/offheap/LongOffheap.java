package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

public class LongOffheap implements Offheap<LongOffheap>
{
    private final long value;

    public LongOffheap(long value)
    {
        this.value = value;
    }

    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Long.BYTES);
        InternalUnsafe.putLong(addr + extraSize, value);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize) {
        InternalUnsafe.free(addr);
    }

    @Override
    public LongOffheap deserialize(long addr) {
        if (0L == addr) {
            return null;
        }

        return new LongOffheap(InternalUnsafe.getLong(addr));
    }

    @Override
    public int compareTo(long addr) {
        if (0L == addr) {
            throw new NullPointerException();
        }
        long v = InternalUnsafe.getLong(addr);
        return value == v ? 0 : (value < v ? -1 : 1);
    }

    @Override
    public int compareTo(LongOffheap o) {
        long v = o.value;
        return value == v ? 0 : (value < v ? -1 : 1);
    }
}
