package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;
import com.alibaba.sdb.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;

import static com.alibaba.sdb.spi.type.UnscaledDecimal128Arithmetic.getLong;

public class LongDecimalOffheap implements Offheap<LongDecimalOffheap>
{
    private final long low;
    private final long high;

    public LongDecimalOffheap(Slice slice)
    {
        low = getLong(slice, 0);
        high = getLong(slice, 1);
    }

    private LongDecimalOffheap(long low, long high)
    {
        this.low = low;
        this.high = high;
    }

    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Long.BYTES + Long.BYTES);
        addr += extraSize;
        InternalUnsafe.putLong(addr, low);
        InternalUnsafe.putLong(addr + Long.BYTES, high);
        return addr - extraSize;
    }

    @Override
    public void free(long addr, int extraSize) {
        InternalUnsafe.free(addr);
    }

    @Override
    public LongDecimalOffheap deserialize(long addr) {
        if (0L == addr) {
            return null;
        }

        return new LongDecimalOffheap(InternalUnsafe.getLong(addr), InternalUnsafe.getLong(addr + Long.BYTES));
    }

    @Override
    public int compareTo(long addr) {
        if (0L == addr) {
            throw new NullPointerException();
        }

        long low = InternalUnsafe.getLong(addr);
        long high = InternalUnsafe.getLong(addr + Long.BYTES);
        return UnscaledDecimal128Arithmetic.compare(this.low, this.high, low, high);
    }

    @Override
    public int compareTo(LongDecimalOffheap that) {
        return UnscaledDecimal128Arithmetic.compare(low, high, that.low, that.high);
    }
}
