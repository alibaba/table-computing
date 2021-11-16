package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

public class BooleanOffheap implements Offheap<BooleanOffheap>
{
    private final byte value;

    public BooleanOffheap(boolean value)
    {
        this.value = value ? 1 : (byte) 0;
    }

    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Byte.BYTES);
        InternalUnsafe.putByte(addr + extraSize, value);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize) {
        InternalUnsafe.free(addr);
    }

    @Override
    public BooleanOffheap deserialize(long addr) {
        if (0L == addr) {
            return null;
        }

        return new BooleanOffheap(InternalUnsafe.getByte(addr) != 0);
    }

    @Override
    public int compareTo(long addr) {
        if (0L == addr) {
            throw new NullPointerException();
        }
        return value - InternalUnsafe.getByte(addr);
    }

    @Override
    public int compareTo(BooleanOffheap o) {
        return value - o.value;
    }
}
