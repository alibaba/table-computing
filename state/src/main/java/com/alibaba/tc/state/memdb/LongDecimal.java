package com.alibaba.tc.state.memdb;

import com.alibaba.sdb.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;

import java.util.Objects;

import static com.alibaba.sdb.spi.type.UnscaledDecimal128Arithmetic.getLong;

public class LongDecimal implements Comparable<LongDecimal> {
    public static final int BYTES = Long.BYTES + Long.BYTES;
    private final long low;
    private final long high;

    public LongDecimal(Slice slice)
    {
        low = getLong(slice, 0);
        high = getLong(slice, 1);
    }

    @Override
    public int compareTo(LongDecimal that)
    {
        return UnscaledDecimal128Arithmetic.compare(low, high, that.low, that.high);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LongDecimal that = (LongDecimal) o;
        return low == that.low &&
                high == that.high;
    }

    @Override
    public int hashCode() {
        return Objects.hash(low, high);
    }

    @Override
    public String toString() {
        return "LongDecimal{" +
                "low=" + low +
                ", high=" + high +
                '}';
    }
}
