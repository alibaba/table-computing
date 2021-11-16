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
import org.openjdk.jol.info.ClassLayout;

import java.util.Optional;
import java.util.function.BiConsumer;

import static com.alibaba.sdb.spi.block.BlockUtil.checkArrayRange;
import static com.alibaba.sdb.spi.block.BlockUtil.checkValidRegion;
import static com.alibaba.sdb.spi.block.BlockUtil.compactBooleanArray;
import static com.alibaba.sdb.spi.block.BlockUtil.compactLongArray;
import static com.alibaba.sdb.spi.block.BlockUtil.countUsedPositions;
import static com.alibaba.sdb.spi.block.BlockUtil.internalPositionInRange;
import static com.alibaba.sdb.spi.block.BlockUtil.sizeOf;
import static java.lang.Integer.bitCount;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class Int128ArrayBlockOffheap extends BlockOffheap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int128ArrayBlockOffheap.class).instanceSize();
    public static final int INT128_BYTES = Long.BYTES + Long.BYTES;

    private final Slice values;

    public Int128ArrayBlockOffheap(int capacity)
    {
        super(capacity, 0);
        this.positionCount = 0;
        this.valueIsNull = null;
        int size = INT128_BYTES * capacity;
        this.values = InternalUnsafe.newSlice(size);

        this.sizeInBytes = size;
        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(this.values);
    }

    public Int128ArrayBlockOffheap(int positionCount, Optional<boolean[]> valueIsNull, long[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    public Int128ArrayBlockOffheap(int positionCount, Optional<Slice> valueIsNull, Slice values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    Int128ArrayBlockOffheap(int arrayOffset, int positionCount, Slice valueIsNull, Slice values)
    {
        super(positionCount, arrayOffset);

        if (arrayOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length()/Long.BYTES - (arrayOffset * 2) < positionCount * 2) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length() - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = (INT128_BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    Int128ArrayBlockOffheap(int positionOffset, int positionCount, boolean[] valueIsNull, long[] values)
    {
        super(positionCount, 0);

        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - (positionOffset * 2) < positionCount * 2) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }

        if (valueIsNull != null && valueIsNull.length - positionOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        int size = INT128_BYTES * positionCount;
        this.values = InternalUnsafe.newSlice(size);
        InternalUnsafe.copyMemory(values, ARRAY_LONG_BASE_OFFSET + positionOffset * INT128_BYTES, null, this.values.getAddress(), size);

        if (valueIsNull != null) {
            size = Byte.BYTES * positionCount;
            this.valueIsNull = InternalUnsafe.newSlice(size);
            InternalUnsafe.copyMemory(valueIsNull, ARRAY_BYTE_BASE_OFFSET + positionOffset * ARRAY_BYTE_INDEX_SCALE, null, this.valueIsNull.getAddress(), size);
        }
        else {
            this.valueIsNull = null;
        }

        sizeInBytes = (INT128_BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(this.valueIsNull) + sizeOf(this.values);
    }

    @Override
    public void appendSlice(Slice slice) {
        checkFull();
        values.setBytes(positionCount * INT128_BYTES, slice);
        positionCount++;
    }

    @Override
    public Slice getValueIsNullSlice()
    {
        return valueIsNull;
    }

    @Override
    public Slice getValuesSlice()
    {
        return values;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (INT128_BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (INT128_BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : INT128_BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, sizeOf(values));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0 && offset != 8) {
            throw new IllegalArgumentException("offset must be 0 or 8");
        }
        return getLongUnchecked(position + arrayOffset, offset);
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull != null && isNullUnchecked(position + arrayOffset);
    }

    private long value(int position, int offset)
    {
        return values.getLong(((position + arrayOffset) * 2 + offset) * ARRAY_LONG_INDEX_SCALE);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeLong(value(position, 0));
        blockBuilder.writeLong(value(position, 1));
        blockBuilder.closeEntry();
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            output.writeLong(value(position, 0));
            output.writeLong(value(position, 1));
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new Int128ArrayBlockOffheap(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new long[] {
                        value(position, 0),
                        value(position, 1)});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        long[] newValues = new long[length * 2];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = isNullUnchecked(position + arrayOffset);
            }
            newValues[i * 2] = value(position, 0);
            newValues[(i * 2) + 1] = value(position, 1);
        }
        return new Int128ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new Int128ArrayBlockOffheap(positionOffset + this.arrayOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        int sliceLength = INT128_BYTES * length;
        if (positionOffset == 0 &&
                length == positionCount &&
                ((values.isCompact() && sliceLength == values.length()) || (!values.isCompact() && sliceLength == values.getRetainedSize())) &&
                (valueIsNull == null || valueIsNull.length() == length)
        ) {
            return this;
        }

        positionOffset += this.arrayOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactBooleanArray(valueIsNull, positionOffset, length);
        long[] newValues = compactLongArray(values, positionOffset * 2, length * 2);

        return new Int128ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return Int128ArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Int128ArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        assert offset == 0 || offset == 8 : "offset must be 0 or 8";
        return this.values.getLong((internalPosition * 2 + bitCount(offset)) * ARRAY_LONG_INDEX_SCALE);
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
        return valueIsNull.getByte(internalPosition) != 0;
    }
}
