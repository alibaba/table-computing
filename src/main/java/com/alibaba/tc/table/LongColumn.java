package com.alibaba.tc.table;

import com.alibaba.tc.ArrayUtil;
import com.alibaba.tc.offheap.AbstractReferenceCounted;
import com.alibaba.tc.offheap.ByteBufferOffheap;
import com.alibaba.tc.offheap.LongBufferOffheap;
import com.alibaba.tc.offheap.ReferenceCounted;

import static com.alibaba.tc.offheap.InternalUnsafe.copyMemory;
import static com.alibaba.tc.offheap.InternalUnsafe.getLong;
import static com.alibaba.tc.offheap.InternalUnsafe.putLong;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class LongColumn extends AbstractReferenceCounted implements ColumnInterface<Long> {
    private LongBufferOffheap values;
    private ByteBufferOffheap valueIsNull;
    private long size;
    private long capacity;

    LongColumn() {
    }

    public LongColumn(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.capacity = capacity;
        values = new LongBufferOffheap(capacity);
    }

    @Override
    public long size() {
        return size;
    }

    private void grow() {
        if (size > capacity) {
            throw new IllegalStateException();
        }
        if (size == capacity) {
            capacity = ArrayUtil.calculateNewSize(capacity);
            if (null != valueIsNull) {
                ByteBufferOffheap tmp = valueIsNull.copy(capacity);
                valueIsNull.release();
                valueIsNull = tmp;
                valueIsNull.init0(size);
            }

            LongBufferOffheap tmp = values.copy(capacity);
            values.release();
            values = tmp;
        }
    }

    @Override
    public long serializeSize() {
        long len = Long.BYTES + size * Long.BYTES;
        if (null != valueIsNull) {
            len += size * Byte.BYTES;
        }
        return len;
    }

    @Override
    public void serialize(byte[] bytes, long offset, long length) {
        long end = offset + length;
        offset += ARRAY_BYTE_BASE_OFFSET;
        putLong(bytes, offset, size);
        offset += Long.BYTES;

        long len = size * Long.BYTES;
        copyMemory(null, values.getAddr(), bytes, offset, len);
        offset += len;

        if (null != valueIsNull) {
            len = size * Byte.BYTES;
            copyMemory(null, valueIsNull.getAddr(), bytes, offset, len);
            offset += len;
        }

        if (offset - ARRAY_BYTE_BASE_OFFSET != end) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public void deserialize(byte[] bytes, long offset, long length) {
        long end = offset + length;

        size = getLong(bytes, offset);
        offset += Long.BYTES;

        capacity = size;
        values = new LongBufferOffheap(capacity);
        long len = size * Long.BYTES;
        copyMemory(bytes, offset, null, values.getAddr(), len);
        offset += len;

        if (offset < end) {
            len = size * Byte.BYTES;
            valueIsNull = new ByteBufferOffheap(capacity);
            copyMemory(bytes, offset, null, valueIsNull.getAddr(), len);
            offset += len;
        }

        if (offset != end) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public void add(Long value) {
        grow();
        if (null == value) {
            if (null == valueIsNull) {
                valueIsNull = new ByteBufferOffheap(capacity);
                valueIsNull.init();
            }
            valueIsNull.set(size, (byte) 1);
        } else {
            values.set(size, (long) value);
        }

        size++;
    }

    @Override
    public Long get(int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        if (valueIsNull != null && valueIsNull.get(index) == 1) {
            return null;
        }

        return values.get(index);
    }

    @Override
    public ReferenceCounted retain() {
        super.retain();
        if (null != valueIsNull) {
            valueIsNull.retain();
        }
        values.retain();
        return this;
    }

    @Override
    public boolean release() {
        super.release();
        if (null != valueIsNull) {
            valueIsNull.release();
        }
        return values.release();
    }
}
