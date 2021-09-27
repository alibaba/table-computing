package com.alibaba.tc.offheap;

import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.tc.offheap.InternalUnsafe.copyMemory;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class VarbyteBufferOffheap extends BufferOffheap {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VarbyteBufferOffheap.class).instanceSize();

    public VarbyteBufferOffheap(long size) {
        super(size);
    }

    public VarbyteBufferOffheap copyFrom(VarbyteBufferOffheap from, long size) {
        copyMemory(from.addr, addr, size);
        return this;
    }

//    public VarbyteBufferOffheap copy(long newSize) {
//        return new VarbyteBufferOffheap(newSize).copyFrom(this, this.size);
//    }

    private void checkArg(long l) {
        if (l < 0) {
            throw new IllegalArgumentException();
        }
    }

    void set(long offset, long srcAddr, long srcLength) {
        checkArg(offset);
        checkArg(srcAddr);
        checkArg(srcLength);

        if (offset + srcLength > size) {
            throw new IndexOutOfBoundsException();
        }
        copyMemory(srcAddr, addr + offset, srcLength);
    }

    public void set(long offset, byte[] srcBytes, long srcOffset, long srcLength) {
        checkArg(offset);
        checkArg(srcOffset);
        checkArg(srcLength);

        if (srcOffset + srcLength > srcBytes.length) {
            throw new IndexOutOfBoundsException();
        }
        if (offset + srcLength > size) {
            throw new IndexOutOfBoundsException();
        }
        copyMemory(srcBytes, ARRAY_BYTE_BASE_OFFSET + srcOffset, null, addr + offset, srcLength);
    }

    public static class Offheap {
        long addr;
        int len;
    }

    Offheap getOffheap(long offset, long length) {
        checkArg(offset);
        checkArg(length);

        if (offset + length > size) {
            throw new IndexOutOfBoundsException();
        }

        int len = (int) length;
        if (len != length) {
            throw new ArithmeticException();
        }

        Offheap offheap = new Offheap();
        offheap.addr = addr + offset;
        offheap.len = len;
        return offheap;
    }

    public byte[] get(long offset, long length) {
        Offheap offheap = getOffheap(offset, length);
        byte[] ret = new byte[offheap.len];
        copyMemory(null, addr + offset, ret, ARRAY_BYTE_BASE_OFFSET, offheap.len);
        return ret;
    }
}