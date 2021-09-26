package com.alibaba.jstream;

import java.nio.ByteBuffer;

import static com.alibaba.jstream.offheap.InternalUnsafe.getInt;
import static com.alibaba.jstream.offheap.InternalUnsafe.putInt;
import static java.lang.String.format;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ArrayUtil {
    //DEFAULT_CAPACITY and INIT_CAPACITY too small will grow too many times to generate more garbage which will increment the GC pressure
    //  but too large will waste more memory space. 64 is a suitable choice.
    public static final int DEFAULT_CAPACITY = 64;
    private static final int INIT_CAPACITY = 64;
    // See java.util.ArrayList for an explanation
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    public static long calculateNewSize(long currentSize)
    {
        // grow array by 50%
        long newSize = currentSize + (currentSize >> 1);

        // verify new size is within reasonable bounds
        if (newSize < INIT_CAPACITY) {
            newSize = INIT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
            if (newSize == currentSize) {
                throw new IllegalArgumentException(format("Can not grow array beyond '%s'", MAX_ARRAY_SIZE));
            }
        }
        return newSize;
    }

    public static byte[] intToBytes(final int data) {
        byte[] ret = new byte[Integer.BYTES];
        putInt(ret, ARRAY_BYTE_BASE_OFFSET, data);
        return ret;
    }

    public static int bytesToInt(final byte[] data) {
        return getInt(data, ARRAY_BYTE_BASE_OFFSET);
    }

    public static byte[] toArr(ByteBuffer byteBuffer) {
        byte[] arr = new byte[byteBuffer.remaining()];
        byteBuffer.get(arr);
        return arr;
    }
}
