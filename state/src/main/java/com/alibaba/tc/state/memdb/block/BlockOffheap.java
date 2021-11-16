package com.alibaba.tc.state.memdb.block;

import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import static com.alibaba.sdb.spi.block.BlockUtil.sizeOf;

public abstract class BlockOffheap implements Block
{
    protected final int arrayOffset;
    protected int positionCount;
    protected final int capacity;

    @Nullable
    protected Slice valueIsNull;

    protected long sizeInBytes;
    protected long retainedSizeInBytes;

    protected BlockOffheap(int capacity, int arrayOffset)
    {
        this.capacity = capacity;
        this.arrayOffset = arrayOffset;
    }

    protected void checkFull()
    {
        if (positionCount < 0 || positionCount >= capacity) {
            throw new IndexOutOfBoundsException(String.format("positionCount: %d, capacity: %d", positionCount, capacity));
        }
    }

    @Override
    public void appendNullValue()
    {
        checkFull();
        if (valueIsNull == null) {
            int size = Byte.BYTES * capacity;
            valueIsNull = InternalUnsafe.newSlice(size);
            InternalUnsafe.setMemory(valueIsNull.getAddress(), size, (byte) 0);
            sizeInBytes += size;
            retainedSizeInBytes += sizeOf(this.valueIsNull);
        }
        valueIsNull.setByte(positionCount, 1);
        positionCount++;
    }
}
