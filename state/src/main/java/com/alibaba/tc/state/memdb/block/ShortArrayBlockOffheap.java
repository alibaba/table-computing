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
import static com.alibaba.sdb.spi.block.BlockUtil.compactShortArray;
import static com.alibaba.sdb.spi.block.BlockUtil.countUsedPositions;
import static com.alibaba.sdb.spi.block.BlockUtil.internalPositionInRange;
import static com.alibaba.sdb.spi.block.BlockUtil.sizeOf;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public class ShortArrayBlockOffheap extends BlockOffheap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ShortArrayBlockOffheap.class).instanceSize();

    private final Slice values;

    public ShortArrayBlockOffheap(int capacity)
    {
        super(capacity, 0);
        this.positionCount = 0;
        this.valueIsNull = null;
        int size = Short.BYTES * capacity;
        this.values = InternalUnsafe.newSlice(size);

        this.sizeInBytes = size;
        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(this.values);
    }

    public ShortArrayBlockOffheap(int positionCount, Optional<boolean[]> valueIsNull, short[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    public ShortArrayBlockOffheap(int positionCount, Optional<Slice> valueIsNull, Slice values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    ShortArrayBlockOffheap(int arrayOffset, int positionCount, Slice valueIsNull, Slice values)
    {
        super(positionCount, arrayOffset);
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length()/ARRAY_SHORT_INDEX_SCALE - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }

        if (valueIsNull != null && valueIsNull.length() - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        this.valueIsNull = valueIsNull;
        this.values = values;

        this.sizeInBytes = (Short.BYTES + Byte.BYTES) * (long) positionCount;
        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(this.valueIsNull) + sizeOf(this.values);
    }

    ShortArrayBlockOffheap(int arrayOffset, int positionCount, boolean[] valueIsNull, short[] values)
    {
        super(positionCount, 0);
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        int size = Short.BYTES * positionCount;
        this.values = InternalUnsafe.newSlice(size);
        InternalUnsafe.copyMemory(values, ARRAY_SHORT_BASE_OFFSET + arrayOffset * ARRAY_SHORT_INDEX_SCALE, null, this.values.getAddress(), size);

        if (valueIsNull != null) {
            size = Byte.BYTES * positionCount;
            this.valueIsNull = InternalUnsafe.newSlice(size);
            InternalUnsafe.copyMemory(valueIsNull, ARRAY_BYTE_BASE_OFFSET + arrayOffset * ARRAY_BYTE_INDEX_SCALE, null, this.valueIsNull.getAddress(), size);
        }
        else {
            this.valueIsNull = null;
        }

        sizeInBytes = (Short.BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(this.valueIsNull) + sizeOf(this.values);
    }

    @Override
    public void appendShort(short aShort)
    {
        checkFull();
        values.setShort(positionCount * Short.BYTES, aShort);
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
        return (Short.BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (Short.BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Short.BYTES;
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
    public short getShort(int position)
    {
        checkReadablePosition(position);
        return value(position);
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

    private short value(int position)
    {
        return getShortUnchecked(position + arrayOffset);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeShort(value(position));
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
            output.writeShort(value(position));
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new ShortArrayBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new short[] {value(position)});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        short[] newValues = new short[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = isNullUnchecked(position + arrayOffset);
            }
            newValues[i] = value(position);
        }
        return new ShortArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        return new ShortArrayBlockOffheap(positionOffset + arrayOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        int sliceLength = ARRAY_SHORT_INDEX_SCALE * length;
        if (positionOffset == 0 &&
                length == positionCount &&
                ((values.isCompact() && sliceLength == values.length()) || (!values.isCompact() && sliceLength == values.getRetainedSize())) &&
                (valueIsNull == null || valueIsNull.length() == length)
        ) {
            return this;
        }

        positionOffset += arrayOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactBooleanArray(valueIsNull, positionOffset, length);
        short[] newValues = compactShortArray(values, positionOffset, length);

        return new ShortArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return ShortArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ShortArrayBlockOffheap{");
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
    public int getOffsetBase()
    {
        return arrayOffset;
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return valueIsNull.getByte(internalPosition * ARRAY_BYTE_INDEX_SCALE) != 0;
    }

    @Override
    public short getShortUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return values.getShort(internalPosition * ARRAY_SHORT_INDEX_SCALE);
    }
}
