package com.alibaba.jstream.table;

import com.alibaba.jstream.ArrayUtil;
import com.alibaba.jstream.offheap.ByteArray;
import com.alibaba.jstream.offheap.ByteBufferOffheap;
import com.alibaba.jstream.offheap.DynamicVarbyteBufferOffheap;
import com.alibaba.jstream.offheap.LongBufferOffheap;
import com.alibaba.jstream.offheap.VarbyteBufferOffheap;

import static com.alibaba.jstream.offheap.InternalUnsafe.copyMemory;
import static com.alibaba.jstream.offheap.InternalUnsafe.getLong;
import static com.alibaba.jstream.offheap.InternalUnsafe.putLong;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class VarbyteColumn implements ColumnInterface {
    private DynamicVarbyteBufferOffheap values;
    private LongBufferOffheap offsets;
    private ByteBufferOffheap valueIsNull;
    private long size;
    private long capacity;

    VarbyteColumn() {
    }

    public VarbyteColumn(int capacity) {
        this.capacity = capacity;
        values = new DynamicVarbyteBufferOffheap(45 * capacity);
        offsets = new LongBufferOffheap(capacity + 1);
        offsets.set(0, 0);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long serializeSize() {
        long len = Long.BYTES + (size + 1) * Long.BYTES + offsets.get(size);
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

        long len = (size + 1) * Long.BYTES;
        copyMemory(null, offsets.getAddr(), bytes, offset, len);
        offset += len;

        len = offsets.get(size);
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
        offsets = new LongBufferOffheap(capacity + 1);
        long len = (size + 1) * Long.BYTES;
        copyMemory(bytes, offset, null, offsets.getAddr(), len);
        offset += len;

        len = offsets.get(size);
        values = new DynamicVarbyteBufferOffheap(len);
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

    private void grow() {
        if (size > capacity) {
            throw new IllegalStateException();
        }
        if (size == capacity) {
            capacity = ArrayUtil.calculateNewSize(capacity);
            if (null != valueIsNull) {
                valueIsNull = valueIsNull.copy(capacity);
                valueIsNull.init0(size);
            }

            offsets = offsets.copy(capacity + 1);
        }
    }

    @Override
    public void add(Comparable comparable) {
        grow();
        if (null == comparable) {
            if (null == valueIsNull) {
                valueIsNull = new ByteBufferOffheap(capacity);
                valueIsNull.init();
            }
            valueIsNull.set(size, (byte) 1);
        } else {
            if (comparable.getClass() == String.class) {
                values.add(((String) comparable));
            } else {
                values.add((ByteArray) comparable);
            }
        }
        addEnd();
    }

    private void addEnd() {
        offsets.set(size + 1, values.size());
        size++;
    }

    public void addOffheap(VarbyteBufferOffheap.Offheap offheap) {
        if (null == offheap) {
            add(null);
        } else {
            grow();
            values.add(offheap);
            addEnd();
        }
    }

    public VarbyteBufferOffheap.Offheap getOffheap(int index) {
        if (checkNull(index)) {
            return null;
        }
        return values.getOffheap(offsets.get(index), offsets.get(index + 1) - offsets.get(index));
    }

    private boolean checkNull(int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        if (valueIsNull != null && valueIsNull.get(index) == 1) {
            return true;
        }

        return false;
    }

    @Override
    public Comparable get(int index) {
        if (checkNull(index)) {
            return null;
        }
        return values.get(offsets.get(index), offsets.get(index + 1) - offsets.get(index));
    }
}
