package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

public class IntegerOffheap implements Offheap<IntegerOffheap>
{
    private final int value;

    public IntegerOffheap(int value)
    {
        this.value = value;
    }

    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Integer.BYTES);
        InternalUnsafe.putInt(addr + extraSize, value);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize) {
        InternalUnsafe.free(addr);
    }

    @Override
    public IntegerOffheap deserialize(long addr) {
        if (0L == addr) {
            return null;
        }

        return new IntegerOffheap(InternalUnsafe.getInt(addr));
    }

    @Override
    public int compareTo(long addr) {
        if (0L == addr) {
            throw new NullPointerException();
        }
        return value - InternalUnsafe.getInt(addr);
    }

    @Override
    public int compareTo(IntegerOffheap o) {
        return value - o.value;
    }
}
