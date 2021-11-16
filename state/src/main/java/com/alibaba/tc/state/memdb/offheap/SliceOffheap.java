package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;
import com.facebook.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.Buffer;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.sdb.spi.block.InternalUnsafe.getByte;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getInt;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getLong;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getObject;
import static com.alibaba.sdb.spi.block.InternalUnsafe.objectFieldOffset;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SliceOffheap implements Offheap<SliceOffheap>
{
    private static final Logger logger = Logger.get(SliceOffheap.class);

    private static class BufferRef {
        long refCount;
        Object directByteBuffer;
    }

    private static final int cpuNum = Runtime.getRuntime().availableProcessors();
    private static final Map<Integer, Map<Long, BufferRef>> addr2Buffer = new HashMap<>();
    private final long addr;
    private final int size;
    private final long originalAddr;
    private final Object directByteBuffer;
    private static long referenceOffset;
    private static long addressOffset;

    static {
        try {
            referenceOffset = objectFieldOffset(Slice.class.getDeclaredField("reference"));
            addressOffset = objectFieldOffset(Buffer.class.getDeclaredField("address"));
            for (int i = 0; i < cpuNum; i++) {
                addr2Buffer.put(i, new HashMap<>());
            }
        } catch (NoSuchFieldException e) {
            logger.error(e);
        }
    }

    public SliceOffheap(Slice slice)
    {
        if (slice.getBase() != null) {
            Slice sliceDirect = Slices.allocateDirect(slice.length());
            sliceDirect.setBytes(0, slice);
            slice = sliceDirect;
        }
        directByteBuffer = getObject(slice, referenceOffset);
        originalAddr = getLong(directByteBuffer, addressOffset);
        addr = slice.getAddress();
        size = slice.length();
    }

    private SliceOffheap(long addr, int size, long originalAddr)
    {
        this.addr = addr;
        this.size = size;
        this.originalAddr = originalAddr;
        Map<Long, BufferRef> addr2Buffer = locateAddr2Buffer(originalAddr);
        synchronized (addr2Buffer) {
            BufferRef bufferRef = addr2Buffer.get(originalAddr);
            this.directByteBuffer = bufferRef.directByteBuffer;
        }
    }

    private Map<Long, BufferRef> locateAddr2Buffer(long addr)
    {
        return addr2Buffer.get(abs(Long.hashCode(addr) % cpuNum));
    }

    @Override
    public int compareTo(long addr) {
        return compareTo(this.addr, this.size, getLong(addr), getInt(addr + Long.BYTES));
    }

    @Override
    public SliceOffheap deserialize(long addr) {
        return new SliceOffheap(
                getLong(addr),
                getInt(addr + Long.BYTES),
                getLong(addr + Long.BYTES + Integer.BYTES)
        );
    }

    //必须确保只调一次alloc和一次serialize，否则会造成内存泄露！！！
    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Long.BYTES + Integer.BYTES + Long.BYTES);
        addr += extraSize;
        InternalUnsafe.putLong(addr, this.addr);
        InternalUnsafe.putInt(addr + Long.BYTES, size);
        InternalUnsafe.putLong(addr + Long.BYTES + Integer.BYTES, originalAddr);

        Map<Long, BufferRef> addr2Buffer = locateAddr2Buffer(originalAddr);
        synchronized (addr2Buffer) {
            BufferRef buffer = addr2Buffer.get(originalAddr);
            if (null == buffer) {
                addr2Buffer.put(originalAddr, new BufferRef(){{this.refCount = 1L; this.directByteBuffer = directByteBuffer;}});
            }
            else {
                buffer.refCount++;
            }
        }

        return addr - extraSize;
    }

    @Override
    public void free(long addr, int extraSize) {
        long originalAddr = getLong(addr + extraSize + Long.BYTES + Integer.BYTES);
        Map<Long, BufferRef> addr2Buffer = locateAddr2Buffer(originalAddr);
        synchronized (addr2Buffer) {
            BufferRef buffer = addr2Buffer.get(originalAddr);
            if (null == buffer) {
                throw new IllegalStateException("try to free an unallocated addr");
            }
            else {
                buffer.refCount--;
                if (0 == buffer.refCount) {
                    addr2Buffer.remove(originalAddr);
                }
            }
        }

        InternalUnsafe.free(addr);
    }

    @Override
    public int compareTo(SliceOffheap that)
    {
        if (this == that) {
            return 0;
        }

        return compareTo(this.addr, this.size, that.addr, that.size);
    }

    private int compareTo(long thisAddr, int thisSize, long thatAddr, int thatSize)
    {
        int compareLength;
        for (compareLength = min(thisSize, thatSize); compareLength >= 8; compareLength -= 8) {
            long thisLong = getLong(thisAddr);
            long thatLong = getLong(thatAddr);
            if (thisLong != thatLong) {
                return longBytesToLong(thisLong) < longBytesToLong(thatLong) ? -1 : 1;
            }

            thisAddr += 8L;
            thatAddr += 8L;
        }

        while (compareLength > 0) {
            byte thisByte = getByte(thisAddr);
            byte thatByte = getByte(thatAddr);
            int v = (thisByte & 255) - (thatByte & 255);
            if (v != 0) {
                return v;
            }

            thisAddr++;
            thatAddr++;
            compareLength--;
        }

        return Integer.compare(thisSize, thatSize);
    }

    private static long longBytesToLong(long bytes) {
        return Long.reverseBytes(bytes) ^ 0x8000000000000000L;
    }

    @Override
    public String toString() {
        byte[] bytes = new byte[size];
        InternalUnsafe.copyMemory(null, addr, bytes, ARRAY_BYTE_BASE_OFFSET, size);
        return new String(bytes);
    }
}
