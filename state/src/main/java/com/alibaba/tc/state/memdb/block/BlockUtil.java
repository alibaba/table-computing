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
import io.airlift.slice.Slices;

import java.util.Arrays;

import static com.alibaba.sdb.spi.block.InternalUnsafe.getIntFromSlice;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public final class BlockUtil
{
    private static final double BLOCK_RESET_SKEW = 1.25;

    private static final int DEFAULT_CAPACITY = 64;
    // See java.util.ArrayList for an explanation
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private BlockUtil()
    {
    }

    static long sizeOf(Slice slice)
    {
        return slice == null ? 0 : slice.getRetainedSize();
    }

    static void checkArrayRange(int[] array, int offset, int length)
    {
        requireNonNull(array, "array is null");
        if (offset < 0 || length < 0 || offset + length > array.length) {
            throw new IndexOutOfBoundsException(format("Invalid offset %s and length %s in array with %s elements", offset, length, array.length));
        }
    }

    static void checkValidRegion(int positionCount, int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException(format("Invalid position %s and length %s in block with %s positions", positionOffset, length, positionCount));
        }
    }

    static void checkValidPositions(boolean[] positions, int positionCount)
    {
        if (positions.length != positionCount) {
            throw new IllegalArgumentException(format("Invalid positions array size %d, actual position count is %d", positions.length, positionCount));
        }
    }

    static void checkValidPosition(int position, int positionCount)
    {
        if (position < 0 || position >= positionCount) {
            throw new IllegalArgumentException(format("Invalid position %s in block with %s positions", position, positionCount));
        }
    }

    static int calculateNewArraySize(int currentSize)
    {
        // grow array by 50%
        long newSize = (long) currentSize + (currentSize >> 1);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
            if (newSize == currentSize) {
                throw new IllegalArgumentException(format("Can not grow array beyond '%s'", MAX_ARRAY_SIZE));
            }
        }
        return (int) newSize;
    }

    static int calculateBlockResetSize(int currentSize)
    {
        long newSize = (long) ceil(currentSize * BLOCK_RESET_SKEW);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
        }
        return (int) newSize;
    }

    static int calculateBlockResetBytes(int currentBytes)
    {
        long newBytes = (long) ceil(currentBytes * BLOCK_RESET_SKEW);
        if (newBytes > MAX_ARRAY_SIZE) {
            return MAX_ARRAY_SIZE;
        }
        return (int) newBytes;
    }

    /**
     * Recalculate the <code>offsets</code> array for the specified range.
     * The returned <code>offsets</code> array contains <code>length + 1</code> integers
     * with the first value set to 0.
     * If the range matches the entire <code>offsets</code> array,  the input array will be returned.
     */
    static int[] compactOffsets(int[] offsets, int index, int length)
    {
        if (index == 0 && offsets.length == length + 1) {
            return offsets;
        }

        int[] newOffsets = new int[length + 1];
        for (int i = 1; i <= length; i++) {
            newOffsets[i] = offsets[index + i] - offsets[index];
        }
        return newOffsets;
    }

    static int[] compactOffsets(Slice offsets, int index, int length)
    {
        int[] newOffsets = new int[length + 1];
        for (int i = 1; i <= length; i++) {
            newOffsets[i] = getIntFromSlice(offsets, index + i) - getIntFromSlice(offsets, index);
        }
        return newOffsets;
    }

    /**
     * Returns a slice containing values in the specified range of the specified slice.
     * If the range matches the entire slice, the input slice will be returned.
     * Otherwise, a copy will be returned.
     */
    static Slice compactSlice(Slice slice, int index, int length)
    {
        if (slice.isCompact() && index == 0 && length == slice.length()) {
            return slice;
        }
        return Slices.copyOf(slice, index, length);
    }

    /**
     * Returns an array containing elements in the specified range of the specified array.
     * If the range matches the entire array, the input array will be returned.
     * Otherwise, a copy will be returned.
     */
    static boolean[] compactArray(boolean[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static boolean[] compactBooleanArray(Slice slice, int index, int length)
    {
        boolean[] newArray = new boolean[length];
        if (ARRAY_BOOLEAN_INDEX_SCALE == ARRAY_BYTE_INDEX_SCALE) {
            InternalUnsafe.copyMemory(slice.getBase(), slice.getAddress() + index, newArray, ARRAY_BOOLEAN_BASE_OFFSET, length);
        }
        else {
            for (int i = 0; i < length; i++) {
                newArray[i] = slice.getByte(index + i) != 0;
            }
        }

        return newArray;
    }

    static byte[] compactArray(byte[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static short[] compactArray(short[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static int[] compactArray(int[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static int[] compactIntArray(Slice slice, int index, int length)
    {
        int[] newArray = new int[length];
        InternalUnsafe.copyMemory(slice.getBase(), slice.getAddress() + index * ARRAY_INT_INDEX_SCALE, newArray, ARRAY_INT_BASE_OFFSET, length * ARRAY_INT_INDEX_SCALE);

        return newArray;
    }

    static short[] compactShortArray(Slice slice, int index, int length)
    {
        short[] newArray = new short[length];
        InternalUnsafe.copyMemory(slice.getBase(), slice.getAddress() + index * ARRAY_SHORT_INDEX_SCALE, newArray, ARRAY_SHORT_BASE_OFFSET, length * ARRAY_SHORT_INDEX_SCALE);

        return newArray;
    }

    static byte[] compactByteArray(Slice slice, int index, int length)
    {
        byte[] newArray = new byte[length];
        InternalUnsafe.copyMemory(slice.getBase(), slice.getAddress() + index, newArray, ARRAY_BYTE_BASE_OFFSET, length);

        return newArray;
    }

    static long[] compactLongArray(Slice slice, int index, int length)
    {
        long[] newArray = new long[length];
        InternalUnsafe.copyMemory(slice.getBase(), slice.getAddress() + index * ARRAY_LONG_INDEX_SCALE, newArray, ARRAY_LONG_BASE_OFFSET, length * ARRAY_LONG_INDEX_SCALE);

        return newArray;
    }

    static long[] compactArray(long[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }

    static int countUsedPositions(boolean[] positions)
    {
        int used = 0;
        for (boolean position : positions) {
            if (position) {
                used++;
            }
        }
        return used;
    }

    /**
     * Returns <tt>true</tt> if the two specified arrays contain the same object in every position.
     * Unlike the {@link Arrays#equals(Object[], Object[])} method, this method compares using reference equals.
     */
    static boolean arraySame(Object[] array1, Object[] array2)
    {
        if (array1 == null || array2 == null || array1.length != array2.length) {
            throw new IllegalArgumentException("array1 and array2 cannot be null and should have same length");
        }

        for (int i = 0; i < array1.length; i++) {
            if (array1[i] != array2[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean internalPositionInRange(int internalPosition, int offset, int positionCount)
    {
        boolean withinRange = internalPosition >= offset && internalPosition < positionCount + offset;
        assert withinRange : format("internalPosition %s is not within range [%s, %s)", internalPosition, offset, positionCount + offset);
        return withinRange;
    }
}
