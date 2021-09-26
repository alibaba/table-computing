package com.alibaba.jstream.offheap;

import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.jstream.offheap.InternalUnsafe.copyMemory;
import static com.alibaba.jstream.offheap.InternalUnsafe.putByte;

public class ByteBufferOffheap extends BufferOffheap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ByteBufferOffheap.class).instanceSize();

    public ByteBufferOffheap(long size)
    {
        super(size);
    }

    private ByteBufferOffheap copyFrom(ByteBufferOffheap from, long size) {
        copyMemory(from.addr, addr, size);
        return this;
    }

    public ByteBufferOffheap copy(long newSize) {
        return new ByteBufferOffheap(newSize).copyFrom(this, this.size);
    }

    public void set(long index, byte value) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if ((index + 1) * Byte.BYTES > size) {
            throw new IndexOutOfBoundsException();
        }
        putByte(addr + index * Byte.BYTES, value);
    }

    public byte get(long index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if ((index + 1) * Byte.BYTES > size) {
            throw new IndexOutOfBoundsException();
        }

        return InternalUnsafe.getByte(addr + index * Byte.BYTES);
    }
}