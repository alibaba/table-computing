/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.tc.state.memdb.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.UnsafeSlice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;

import static com.alibaba.sdb.spi.block.BlockUtil.checkArrayRange;
import static com.alibaba.sdb.spi.block.BlockUtil.checkValidRegion;
import static com.alibaba.sdb.spi.block.BlockUtil.compactBooleanArray;
import static com.alibaba.sdb.spi.block.BlockUtil.compactOffsets;
import static com.alibaba.sdb.spi.block.BlockUtil.compactSlice;
import static com.alibaba.sdb.spi.block.BlockUtil.internalPositionInRange;
import static com.alibaba.sdb.spi.block.BlockUtil.sizeOf;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getBoolFromSlice;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getIntFromSlice;
import static com.alibaba.sdb.spi.block.InternalUnsafe.setIntToSlice;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class VariableWidthBlockOffheap
        extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlockOffheap.class).instanceSize();

    private final int arrayOffset;
    private int positionCount;
    private Slice slice;
    private final Slice offsets;
    @Nullable
    private Slice valueIsNull;
    private final int capacity;

    private long retainedSizeInBytes;
    private long sizeInBytes;

    public VariableWidthBlockOffheap(int capacity)
    {
        this.arrayOffset = 0;
        this.positionCount = 0;
        this.capacity = capacity;
        this.valueIsNull = null;
        int size = Integer.BYTES * (capacity + 1);
        this.offsets = InternalUnsafe.newSlice(size);
        this.offsets.setInt(0, 0);

        this.sizeInBytes = size;

        //每个varchar至少先分配45个字节(来自于mysql默认的varchar(45)，这对于大部分情况应该都不需要growCapacity了)
        size = capacity * 45;
        this.slice = InternalUnsafe.newSlice(size);
        updateRetainedSize();
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(offsets) + slice.getRetainedSize() + sizeOf(valueIsNull);
    }

    public VariableWidthBlockOffheap(int positionCount, Slice slice, int[] offsets, Optional<boolean[]> valueIsNull)
    {
        this(0, positionCount, slice, offsets, valueIsNull.orElse(null));
    }

    public VariableWidthBlockOffheap(int positionCount, Slice slice, Slice offsets, Optional<Slice> valueIsNull)
    {
        this(0, positionCount, slice, offsets, valueIsNull.orElse(null));
    }

    VariableWidthBlockOffheap(int arrayOffset, int positionCount, Slice slice, Slice offsets, Slice valueIsNull) {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (slice == null) {
            throw new IllegalArgumentException("slice is null");
        }

        if (offsets.length()/ARRAY_INT_INDEX_SCALE - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }

        if (valueIsNull != null && valueIsNull.length() - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }

        this.slice = slice;
        this.offsets = offsets;
        this.valueIsNull = valueIsNull;
        this.capacity = positionCount;

        sizeInBytes = getIntFromSlice(offsets, arrayOffset + positionCount) - getIntFromSlice(offsets, arrayOffset) +
                ((Integer.BYTES + Byte.BYTES) * (long) positionCount) +
                Integer.BYTES;
        updateRetainedSize();
    }

    VariableWidthBlockOffheap(int arrayOffset, int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = 0;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (slice == null) {
            throw new IllegalArgumentException("slice is null");
        }

        if (offsets.length - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }

        int[] newOffsets = compactOffsets(offsets, arrayOffset, positionCount);
        int size = Integer.BYTES * (positionCount + 1);
        this.offsets = InternalUnsafe.newSlice(size);
        InternalUnsafe.copyMemory(newOffsets, ARRAY_INT_BASE_OFFSET, null, this.offsets.getAddress(), size);

        size = Byte.BYTES * newOffsets[positionCount];
        if (size == 0) {
            this.slice = EMPTY_SLICE;
        } else {
            this.slice = InternalUnsafe.newSlice(size);
            this.slice.setBytes(0, slice, offsets[arrayOffset], newOffsets[positionCount]);
        }

        if (valueIsNull != null) {
            size = Byte.BYTES * positionCount;
            this.valueIsNull = InternalUnsafe.newSlice(size);
            InternalUnsafe.copyMemory(valueIsNull, ARRAY_BYTE_BASE_OFFSET + arrayOffset * ARRAY_BYTE_INDEX_SCALE, null, this.valueIsNull.getAddress(), size);
        } else {
            this.valueIsNull = null;
        }

        this.capacity = positionCount;

        sizeInBytes = offsets[arrayOffset + positionCount] - offsets[arrayOffset] +
                ((Integer.BYTES + Byte.BYTES) * (long) positionCount) +
                Integer.BYTES;
        updateRetainedSize();
    }

    private void checkFull()
    {
        if (positionCount < 0 || positionCount >= capacity) {
            throw new IndexOutOfBoundsException(String.format("positionCount: %d, capacity: %d", positionCount, capacity));
        }
    }

    @Override
    public Block compactOnOffheap() {
        int size = getIntFromSlice(offsets, positionCount);
        Slice slice = InternalUnsafe.newSlice(size);
        InternalUnsafe.copyMemory(this.slice.getAddress(), slice.getAddress(), size);
        this.slice = slice;
        updateRetainedSize();
        return this;
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
            updateRetainedSize();
        }
        valueIsNull.setByte(positionCount, 1);
        setIntToSlice(offsets, positionCount + 1, getIntFromSlice(offsets, positionCount));
        positionCount++;
    }

    private void ensureCapacity(int length)
    {
        int needSize = getIntFromSlice(offsets, positionCount) + length;
        if (needSize <= slice.length()) {
            return;
        }
        int newSize = slice.length() * 2;
        newSize = newSize >= needSize ? newSize : needSize;

        //注意: getRawSlice和getSlice这两个方法可能返回旧的slice，在旧的slice上读positionCount增加之后的部分可能会抛越界异常
        Slice slice = InternalUnsafe.newSlice(newSize);
        InternalUnsafe.copyMemory(this.slice.getAddress(), slice.getAddress(), needSize - length);
        this.slice = slice;

        updateRetainedSize();
    }

    @Override
    public void appendSlice(Slice slice)
    {
        checkFull();
        ensureCapacity(slice.length());
        this.slice.setBytes(getIntFromSlice(offsets, positionCount), slice);
        setIntToSlice(offsets, positionCount + 1, getIntFromSlice(offsets, positionCount) + slice.length());

        sizeInBytes += slice.length();

        positionCount++;
    }

    @Override
    public final int getPositionOffset(int position)
    {
        checkReadablePosition(position);
        return getIntFromSlice(offsets, position + arrayOffset);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(position);
        return getSliceLengthUnchecked(position + arrayOffset);
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull != null && getBoolFromSlice(valueIsNull, position + arrayOffset);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return getIntFromSlice(offsets, arrayOffset + position + length) - getIntFromSlice(offsets, arrayOffset + position) +
                    ((Integer.BYTES + Byte.BYTES) * (long) length) +
                    Integer.BYTES;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        long sizeInBytes = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                sizeInBytes += getIntFromSlice(offsets, arrayOffset + i + 1) - getIntFromSlice(offsets, arrayOffset + i);
            }
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount + Integer.BYTES;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(slice, sizeOf(slice));
        consumer.accept(offsets, sizeOf(offsets));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int finalLength = 0;
        for (int i = offset; i < offset + length; i++) {
            finalLength += getSliceLength(positions[i]);
        }
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        int[] newOffsets = new int[length + 1];
        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (!isEntryNull(position)) {
                newSlice.writeBytes(slice, getPositionOffset(position), getSliceLength(position));
            }
            else if (newValueIsNull != null) {
                newValueIsNull[i] = true;
            }
            newOffsets[i + 1] = newSlice.size();
        }
        return new VariableWidthBlock(0, length, newSlice.slice(), newOffsets, newValueIsNull);
    }

    @Override
    public Slice getRawSlice(int position)
    {
        return slice;
    }

    @Override
    public Slice getOffsetsSlice()
    {
        return offsets;
    }

    @Override
    public Slice getValueIsNullSlice()
    {
        return valueIsNull;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new VariableWidthBlockOffheap(positionOffset + arrayOffset, length, slice, offsets, valueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        int sliceLength = getIntFromSlice(offsets, length);
        if (positionOffset == 0 &&
                length == positionCount &&
                offsets.length() == ARRAY_INT_INDEX_SCALE * (length + 1) &&
                ((slice.isCompact() && sliceLength == slice.length()) || (!slice.isCompact() && sliceLength == slice.getRetainedSize())) &&
                (valueIsNull == null || valueIsNull.length() == length * ARRAY_BYTE_INDEX_SCALE)
        ) {
            return this;
        }

        positionOffset += arrayOffset;

        int[] newOffsets = compactOffsets(offsets, positionOffset, length);
        Slice newSlice = compactSlice(slice, getIntFromSlice(offsets, positionOffset), newOffsets[length]);
        boolean[] newValueIsNull = valueIsNull == null ? null : compactBooleanArray(valueIsNull, positionOffset, length);

        return new VariableWidthBlock(0, length, newSlice, newOffsets, newValueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockOffheap{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public byte getByteUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return UnsafeSlice.getByteUnchecked(getRawSlice(internalPosition), getIntFromSlice(offsets, internalPosition));
    }

    @Override
    public short getShortUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return UnsafeSlice.getShortUnchecked(getRawSlice(internalPosition), getIntFromSlice(offsets, internalPosition));
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return UnsafeSlice.getIntUnchecked(getRawSlice(internalPosition), getIntFromSlice(offsets, internalPosition));
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return UnsafeSlice.getLongUnchecked(getRawSlice(internalPosition), getIntFromSlice(offsets, internalPosition));
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return UnsafeSlice.getLongUnchecked(getRawSlice(internalPosition), getIntFromSlice(offsets, internalPosition) + offset);
    }

    @Override
    public Slice getSliceUnchecked(int internalPosition, int offset, int length)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return getRawSlice(internalPosition).slice(getIntFromSlice(offsets, internalPosition) + offset, length);
    }

    @Override
    public int getSliceLengthUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return getIntFromSlice(offsets, internalPosition + 1) - getIntFromSlice(offsets, internalPosition);
    }

    @Override
    public int getOffsetBase()
    {
        return arrayOffset;
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return getBoolFromSlice(valueIsNull, internalPosition);
    }
}
