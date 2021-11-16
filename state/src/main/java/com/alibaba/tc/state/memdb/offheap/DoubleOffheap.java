package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

public class DoubleOffheap implements Offheap<DoubleOffheap>
{
    private final double value;

    public DoubleOffheap(double value)
    {
        this.value = value;
    }

    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Double.BYTES);
        InternalUnsafe.putDouble(addr + extraSize, value);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize) {
        InternalUnsafe.free(addr);
    }

    @Override
    public DoubleOffheap deserialize(long addr) {
        if (0L == addr) {
            return null;
        }

        return new DoubleOffheap(InternalUnsafe.getInt(addr));
    }

    @Override
    public int compareTo(long addr) {
        if (0L == addr) {
            throw new NullPointerException();
        }
        double v = InternalUnsafe.getDouble(addr);
        return value == v ? 0 : (value < v ? -1 : 1);
    }

    @Override
    public int compareTo(DoubleOffheap o) {
        double v = o.value;
        return value == v ? 0 : (value < v ? -1 : 1);
    }
}
