package com.alibaba.jstream.offheap;

import static com.alibaba.jstream.ArrayUtil.DEFAULT_CAPACITY;
import static com.alibaba.jstream.ArrayUtil.calculateNewSize;

public class DynamicVarbyteBufferOffheap {
    private VarbyteBufferOffheap varbyteBufferOffheap;
    private long size;

    public DynamicVarbyteBufferOffheap() {
        this(45 * DEFAULT_CAPACITY);
    }

    public DynamicVarbyteBufferOffheap(long capacity) {
        varbyteBufferOffheap = new VarbyteBufferOffheap(capacity);
    }

    public long size() {
        return size;
    }

    public long getAddr() {
        return varbyteBufferOffheap.getAddr();
    }

    private void grow(int length) {
        if (length < 0) {
            throw new IllegalArgumentException();
        }

        if (size + length > varbyteBufferOffheap.getSize()) {
            long newSize = calculateNewSize(varbyteBufferOffheap.getSize());
            if (size + length > newSize) {
                newSize = size + length;
            }
            varbyteBufferOffheap = new VarbyteBufferOffheap(newSize).copyFrom(varbyteBufferOffheap, size);
        }
    }

    public void add(String string) {
        add(string.getBytes());
    }

    public void add(ByteArray byteArray) {
        add(byteArray.getBytes(), byteArray.getOffset(), byteArray.getLength());
    }

    public void add(byte[] bytes) {
        add(bytes, 0, bytes.length);
    }

    public void add(VarbyteBufferOffheap.Offheap offheap) {
        grow(offheap.len);
        varbyteBufferOffheap.set(size, offheap.addr, offheap.len);
        size += offheap.len;
    }

    public void add(byte[] bytes, int offset, int length) {
        grow(length);
        varbyteBufferOffheap.set(size, bytes, offset, length);
        size += length;
    }

    public VarbyteBufferOffheap.Offheap getOffheap(long offset, long length) {
        return varbyteBufferOffheap.getOffheap(offset, length);
    }

    public ByteArray get(long offset, long length) {
        return new ByteArray(varbyteBufferOffheap.get(offset, length));
    }
}
