package com.alibaba.jstream.offheap;

import static java.lang.Integer.min;
import static java.util.Objects.requireNonNull;

public class ByteArray implements Comparable<ByteArray> {
    private final byte[] bytes;
    private final int offset;
    private final int length;
    private int hash; // Default to 0

    public ByteArray(String string) {
        this(string.getBytes());
    }

    public ByteArray(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public ByteArray(byte[] bytes, int offset, int length) {
        if (offset < 0) {
            throw new IllegalArgumentException();
        }
        if (length < 0) {
            throw new IllegalArgumentException();
        }
        this.bytes = requireNonNull(bytes);
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int compareTo(ByteArray that) {
        if (this == that) {
            return 0;
        }

        for (int i = 0; i < min(this.length, that.length); i++) {
            if (this.bytes[this.offset + i] < that.bytes[that.offset + i]) {
                return -1;
            }
            if (this.bytes[this.offset + i] > that.bytes[that.offset + i]) {
                return 1;
            }
        }

        return this.length - that.length;
    }

    @Override
    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof ByteArray) {
            ByteArray that = (ByteArray)anObject;
            if (length == that.length) {
                for (int i = 0; i < length; i++) {
                    if (this.bytes[this.offset + i] != that.bytes[that.offset + i]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && length > 0) {
            for (int i = 0; i < length; i++) {
                h = 31 * h + bytes[offset + i];
            }
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return new String(bytes, offset, length);
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }
}
