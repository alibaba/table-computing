package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.alibaba.sdb.spi.block.InternalUnsafe.getInt;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getLong;
import static com.alibaba.sdb.spi.block.InternalUnsafe.putInt;
import static com.alibaba.sdb.spi.block.InternalUnsafe.putLong;
import static com.alibaba.sdb.spi.block.InternalUnsafe.setMemory;

public class HashSetOffheap<E extends Offheap<E> & HashCoder>
    implements Iterable<E>, Serializer<HashSetOffheap>
{
    //一个next指针加一个哈希值
    private static final int PREFIX_BYTES = Long.BYTES + Integer.BYTES;
    private final long handle;
//    private long table = 0L;
//    private int size = 0;
//    private int capacity = 0;
//    private int threshold = 0;
    private final short prototype;
    private final long refCount;

    private static final Property[] properties = new Property[3];
    private static final long INSTANCE_SIZE = initProperties(HashSetOffheap.class, properties);

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    @Override
    public HashSetOffheap deserialize(long addr)
    {
        return Serializer.deserialize(HashSetOffheap.class, properties, addr);
    }

    @Override
    public long allocAndSerialize(int extraSize)
    {
        InternalUnsafe.getAndAddLong(refCount, 1);
        long addr = InternalUnsafe.alloc(extraSize + INSTANCE_SIZE);
        serialize(properties, addr + extraSize);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize)
    {
        HashSetOffheap hashSetOffheap = deserialize(addr + extraSize);
        long count = InternalUnsafe.getAndAddLong(hashSetOffheap.refCount, -1);
        if (count <= 1) {
            hashSetOffheap.free();
        }
        InternalUnsafe.free(addr);
    }

    private long table()
    {
        return getLong(handle);
    }

    private void table(long table)
    {
        putLong(handle, table);
    }

    public int size()
    {
        return getInt(handle + Long.BYTES);
    }

    private void size(int size)
    {
        putInt(handle + Long.BYTES, size);
    }

    private int capacity()
    {
        return getInt(handle + Long.BYTES + Integer.BYTES);
    }

    private void capacity(int capacity)
    {
        putInt(handle + Long.BYTES + Integer.BYTES, capacity);
    }

    private int threshold()
    {
        return getInt(handle + Long.BYTES + Integer.BYTES + Integer.BYTES);
    }

    private void threshold(int threshold)
    {
        putInt(handle + Long.BYTES + Integer.BYTES + Integer.BYTES, threshold);
    }

    public HashSetOffheap(Class prototype)
    {
        int size = Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES;
        handle = InternalUnsafe.alloc(size);
        setMemory(handle, size, (byte) 0);
        this.prototype = OffheapClass.putClass(prototype);
        //一个8字节的引用计数，引用计数为0的时候才可以释放
        refCount = InternalUnsafe.alloc(Long.BYTES);
        InternalUnsafe.putLong(refCount, 1);
        resize();
    }

    @Override
    public Iterator<E> iterator() {
        return new KeyIterator();
    }

    /**
     * Computes key.hashCode() and spreads (XORs) higher bits of hash
     * to lower.  Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a tradeoff between speed, utility, and
     * quality of bit-spreading. Because many common sets of hashes
     * are already reasonably distributed (so don't benefit from
     * spreading), and because we use trees to handle large sets of
     * collisions in bins, we just XOR some shifted bits in the
     * cheapest possible way to reduce systematic lossage, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     */
    private final int hash(E key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    private E prototype()
    {
        return (E) OffheapClass.getObjectById(prototype);
    }

    private void deleteElement(long addr)
    {
        prototype().free(addr, PREFIX_BYTES);
    }

    public boolean isEmpty()
    {
        return 0 == size();
    }

    private boolean resize()
    {
        if (size() < threshold()) {
            return false;
        }

        int oldCap = capacity();
        int oldThr = threshold();
        int newCap, newThr = 0;
        if (oldCap > 0) {
            if (oldCap >= MAXIMUM_CAPACITY) {
                threshold(Integer.MAX_VALUE);
                return false;
            }
            if ((newCap = oldCap << 1) < MAXIMUM_CAPACITY &&
                    oldCap >= DEFAULT_INITIAL_CAPACITY) {
                // double threshold
                newThr = oldThr << 1;
            }
        }
        else {
            // zero initial threshold signifies using defaults
            newCap = DEFAULT_INITIAL_CAPACITY;
            newThr = (int)(DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
        }

        threshold(newThr);
        capacity(newCap);
        long oldTable = table();
        long size = Long.BYTES * newCap;
        table(InternalUnsafe.alloc(size));
        InternalUnsafe.setMemory(table(), size, (byte) 0);
        for (int i = 0; i < oldCap; i++) {
            long head = getLong(oldTable + i * Long.BYTES);
            while (0L != head) {
                long next = getLong(head);
                add(head);
                head = next;
            }
        }
        InternalUnsafe.free(oldTable);

        return true;
    }

    private void add(long addr)
    {
        int hash = getHash(addr);
        int position = position(hash);
        long head = getHead(position);
        setNext(addr, head);
        setHead(position, addr);
    }

    private int getHash(long addr)
    {
        return getInt(addr + Long.BYTES);
    }

    private void setHash(long addr, E e)
    {
        putInt(addr + Long.BYTES, hash(e));
    }

    private long getHead(int position)
    {
        return getLong(table() + position * Long.BYTES);
    }

    private void setHead(int position, long addr)
    {
        InternalUnsafe.putLong(table() + position * Long.BYTES, addr);
    }

    private void setNext(long addr, long next)
    {
        InternalUnsafe.putLong(addr, next);
    }

    private long getNext(long addr)
    {
        return getLong(addr);
    }

    private int position(int hash)
    {
        return hash & (capacity() - 1);
    }

    public boolean add(E key)
    {
        int position = position(hash(key));
        long head = getHead(position);
        if (contains(head, key)) {
            return false;
        }

        long addr = key.allocAndSerialize(PREFIX_BYTES);
        setHash(addr, key);
        setNext(addr, head);
        setHead(position, addr);

        size(size() + 1);
        resize();

        return true;
    }

    private boolean contains(long head, E key)
    {
        while (0L != head) {
            if (0 == key.compareTo(head + PREFIX_BYTES)) {
                return true;
            }
            head = getNext(head);
        }
        return false;
    }

    public boolean contains(E key)
    {
        int position = position(hash(key));
        long head = getHead(position);
        return contains(head, key);
    }

    public boolean remove(E key)
    {
        int position = position(hash(key));
        long pre = getHead(position);
        long p = pre;
        pre = 0L;
        while (0L != p) {
            long next = getNext(p);
            if (0 == key.compareTo(p + PREFIX_BYTES)) {
                if (0L == pre) {
                    setHead(position, next);
                }
                else {
                    setNext(pre, next);
                }
                deleteElement(p);
                size(size() - 1);
                return true;
            }
            pre = p;
            p = next;
        }
        return false;
    }

    private void free()
    {
        for (int i = 0; i < capacity(); i++) {
            long head = getHead(i);
            while (0L != head) {
                long next = getNext(head);
                deleteElement(head);
                head = next;
            }
        }
        InternalUnsafe.free(table());
        InternalUnsafe.free(handle);
        InternalUnsafe.free(refCount);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        long count = InternalUnsafe.getAndAddLong(refCount, -1);
        if (count > 1) {
            return;
        }

        free();
    }

    private final class KeyIterator implements Iterator<E> {
        long next;
        int expectedSize;
        int index;

        KeyIterator()
        {
            expectedSize = size();
            next = 0L;
            index = 0;
            if (table() != 0L && size() > 0) { // advance to first entry
                do {} while (index < capacity() && (next = getHead(index++)) == 0L);
            }
        }

        @Override
        public boolean hasNext() {
            return next != 0L;
        }

        @Override
        public E next() {
            long e = next;
            if (size() != expectedSize) {
                throw new ConcurrentModificationException();
            }
            if (e == 0L) {
                throw new NoSuchElementException();
            }
            if ((next = getNext(e)) == 0L && table() != 0L) {
                do {} while (index < capacity() && (next = getHead(index++)) == 0L);
            }
            return prototype().deserialize(e + PREFIX_BYTES);
        }
    }
}
