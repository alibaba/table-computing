package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.sdb.spi.block.InternalUnsafe.getByte;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getInt;
import static com.alibaba.sdb.spi.block.InternalUnsafe.getLong;
import static com.alibaba.sdb.spi.block.InternalUnsafe.putByte;
import static com.alibaba.sdb.spi.block.InternalUnsafe.putInt;
import static com.alibaba.sdb.spi.block.InternalUnsafe.putLong;
import static com.alibaba.sdb.spi.block.InternalUnsafe.setMemory;

public class SkipListMapOffheap<K extends Offheap<K>, V extends Serializer<V>>
        implements Iterable<SkipListMapOffheap.Entry<K, V>>, Serializer<SkipListMapOffheap>
{
    private static final int LEVEL_HEIGHT_OFFSET = Short.BYTES + Short.BYTES;
    //最多 4^32=2^64 个元素
    private static final int MAX_LEVEL = 32;

    private final long handle;
    private final long header;
//    private long tail;
//    private int size;
//    private int levelHeight;
//    private boolean hasNull;
    private final long nullKeyNode;
    private final long refCount;

    private static final Property[] properties = new Property[4];
    private static final long INSTANCE_SIZE = Serializer.initProperties(SkipListMapOffheap.class, properties);

    //node 结构：
//    class Node {
//        short keyClassId;
//        short valueClassId;
//        byte levelHeight;
//        long[] level;
//        long value;
//        long key;
//    }

    @Override
    public SkipListMapOffheap deserialize(long addr)
    {
        return Serializer.deserialize(SkipListMapOffheap.class, properties, addr);
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
        SkipListMapOffheap skipListMapOffheap = deserialize(addr + extraSize);
        long count = InternalUnsafe.getAndAddLong(skipListMapOffheap.refCount, -1);
        if (count <= 1) {
            skipListMapOffheap.free();
        }
        InternalUnsafe.free(addr);
    }

    private long tail()
    {
        return getLong(handle);
    }

    private void tail(long tail)
    {
        putLong(handle, tail);
    }

    public int size()
    {
        return getInt(handle + Long.BYTES);
    }

    private void size(int size)
    {
        putInt(handle + Long.BYTES, size);
    }

    private int levelHeight()
    {
        return getInt(handle + Long.BYTES + Integer.BYTES);
    }

    private void levelHeight(int levelHeight)
    {
        putInt(handle + Long.BYTES + Integer.BYTES, levelHeight);
    }

    public boolean hasNull()
    {
        return getByte(handle + Long.BYTES + Integer.BYTES + Integer.BYTES) != 0;
    }

    private void hasNull(boolean hasNull)
    {
        putByte(handle + Long.BYTES + Integer.BYTES + Integer.BYTES, hasNull ? 1 : (byte) 0);
    }

    public SkipListMapOffheap()
    {
        int size = Long.BYTES + Integer.BYTES + Integer.BYTES + Byte.BYTES;
        handle = InternalUnsafe.alloc(size);
        setMemory(handle, size, (byte) 0);

        int prefixBytes = prefixBytes(MAX_LEVEL);
        header = InternalUnsafe.alloc(prefixBytes);
        InternalUnsafe.setMemory(header, prefixBytes, (byte) 0);
        setLevelHeight(header, MAX_LEVEL);

        nullKeyNode = InternalUnsafe.alloc(prefixBytes);
        InternalUnsafe.setMemory(nullKeyNode, prefixBytes, (byte) 0);
        setLevelHeight(nullKeyNode, MAX_LEVEL);
        setValue(nullKeyNode, null);

        //一个8字节的引用计数，引用计数为0的时候才可以释放
        refCount = InternalUnsafe.alloc(Long.BYTES);
        InternalUnsafe.putLong(refCount, 1);

        levelHeight(1);
    }

    public static final class Entry<K, V>
    {
        final K key;
        final V value;

        Entry(K key, V value)
        {
            this.key = key;
            this.value = value;
        }

        public K getKey()
        {
            return key;
        }

        public V getValue()
        {
            return value;
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator()
    {
        return new EntryIterator(getLevel(header, 0), 0L);
    }

    private int level()
    {
        int rnd = ThreadLocalRandom.current().nextInt();
        int level = 1;

        // test highest and lowest bits
        if ((rnd & 0x80000001) == 0) {
            //4的指数次，数学期望是一个以4为底的对数模型
            while (((rnd >>>= 1) & 1) != 0 && ((rnd >>>= 1) & 1) != 0) {
                ++level;
            }
        }
        return level > MAX_LEVEL ? MAX_LEVEL : level;
    }

    private int prefixBytes(int levelHeight)
    {
        //两个二字节的classId + 一个字节的levelHeight + levelHeight个level指针 + 一个value指针
        return LEVEL_HEIGHT_OFFSET + Byte.BYTES + levelHeight * Long.BYTES + Long.BYTES;
    }

    private void setLevelHeight(long nodeAddr, int levelHeight)
    {
        putByte(nodeAddr + LEVEL_HEIGHT_OFFSET, (byte) levelHeight);
    }

    private void setLevel(long nodeAddr, int level, long forward)
    {
        InternalUnsafe.putLong(nodeAddr + LEVEL_HEIGHT_OFFSET + Byte.BYTES + level * Long.BYTES, forward);
    }

    private long getLevel(long nodeAddr, int level)
    {
        return InternalUnsafe.getLong(nodeAddr + LEVEL_HEIGHT_OFFSET + Byte.BYTES + level * Long.BYTES);
    }

    private int getLevelHeight(long nodeAddr)
    {
        return getByte(nodeAddr + LEVEL_HEIGHT_OFFSET);
    }

    private void setValue(long nodeAddr, V value)
    {
        long valueAddr = 0L;
        if (null == value) {
            setValueClassId(nodeAddr, (short) -1);
        }
        else {
            setValueClassId(nodeAddr, OffheapClass.putClass(value.getClass()));
            valueAddr = value.allocAndSerialize(0);
        }
        InternalUnsafe.putLong(nodeAddr + LEVEL_HEIGHT_OFFSET + Byte.BYTES + getLevelHeight(nodeAddr) * Long.BYTES, valueAddr);
    }

    private long valueAddr(long nodeAddr)
    {
        return InternalUnsafe.getLong(nodeAddr + LEVEL_HEIGHT_OFFSET + Byte.BYTES + getLevelHeight(nodeAddr) * Long.BYTES);
    }

    private long keyAddr(long nodeAddr)
    {
        int levelHeight = getLevelHeight(nodeAddr);
        return nodeAddr + LEVEL_HEIGHT_OFFSET + Byte.BYTES + levelHeight * Long.BYTES + Long.BYTES;
    }

    private void setKeyClassId(long nodeAddr, short id)
    {
        InternalUnsafe.putShort(nodeAddr, id);
    }

    private short keyClassId(long nodeAddr)
    {
        return InternalUnsafe.getShort(nodeAddr);
    }

    private K prototypeKey(long nodeAddr)
    {
        return (K) OffheapClass.getObjectById(keyClassId(nodeAddr));
    }

    private void setValueClassId(long nodeAddr, short id)
    {
        InternalUnsafe.putShort(nodeAddr + Short.BYTES, id);
    }

    private short valueClassId(long nodeAddr)
    {
        return InternalUnsafe.getShort(nodeAddr + Short.BYTES);
    }

    private V prototypeValue(long nodeAddr)
    {
        return (V) OffheapClass.getObjectById(valueClassId(nodeAddr));
    }

    private void freeValue(long nodeAddr)
    {
        long addr = valueAddr(nodeAddr);
        if (0L != addr) {
            prototypeValue(nodeAddr).free(addr, 0);
        }
    }

    private void free(long nodeAddr)
    {
        freeValue(nodeAddr);
        prototypeKey(nodeAddr).free(nodeAddr, prefixBytes(getLevelHeight(nodeAddr)));
    }

    private void free()
    {
        InternalUnsafe.free(refCount);

        freeValue(nullKeyNode);
        InternalUnsafe.free(nullKeyNode);

        long next = getLevel(header, 0);
        InternalUnsafe.free(header);
        long p = next;

        while (p != 0L) {
            next = getLevel(p, 0);
            free(p);
            p = next;
        }

        InternalUnsafe.free(handle);
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

    public V remove(K key)
    {
        if (null == key) {
            if (hasNull()) {
                size(size() - 1);
                hasNull(false);
            }
            return getSetValue(nullKeyNode, null);
        }

        long[] update = locate(key, true);
        long p = getLevel(update[0], 0);

        assert OffheapClass.putClass(key.getClass()) == keyClassId(p);
        if (p != header && key.compareTo(keyAddr(p)) == 0) {
            V v = value(p);
            for (int i = 0; i < getLevelHeight(p); i++) {
                setLevel(update[i], i, getLevel(p, i));
            }
            free(p);
            if (p == tail()) {
                tail(update[0]);
            }

            size(size() - 1);

            return v;
        }

        return null;
    }

    private final long[] locate(ComparableOffheap key, boolean lowerOrEqual)
    {
        long[] update = new long[MAX_LEVEL];
        long p = header;
        for (int i = levelHeight() - 1; i >= 0; i--) {
            long forward = getLevel(p, i);
            while (forward != 0L) {
                assert OffheapClass.putClass(key.getClass()) == keyClassId(forward);
                if (lowerOrEqual) {
                     if (key.compareTo(keyAddr(forward)) <= 0) {
                         break;
                     }
                }
                else {
                    if (key.compareTo(keyAddr(forward)) < 0) {
                        break;
                    }
                }
                p = forward;
                forward = getLevel(p, i);
            }
            update[i] = p;
        }

        assert update[0] != 0L;

        return update;
    }

    private V value(long nodeAddr)
    {
        long addr = valueAddr(nodeAddr);
        if (addr == 0L) {
            return null;
        }
        return prototypeValue(nodeAddr).deserialize(addr);
    }

    private V getSetValue(long nodeAddr, V value)
    {
        V v = value(nodeAddr);
        freeValue(nodeAddr);
        setValue(nodeAddr, value);
        return v;
    }

    public V get(K key)
    {
        if (null == key) {
            return value(nullKeyNode);
        }

        long[] update = locate(key, false);
        long p = update[0];

        if (p != header) {
            assert OffheapClass.putClass(key.getClass()) == keyClassId(p);
        }

        if (p != header && key.compareTo(keyAddr(p)) == 0) {
            return value(p);
        }

        return null;
    }

    public V put(K key, V value)
    {
        if (null == key) {
            if (!hasNull()) {
                size(size() + 1);
                hasNull(true);
            }
            return getSetValue(nullKeyNode, value);
        }

        long[] update = locate(key, false);
        long p = update[0];

        if (p != header) {
            assert OffheapClass.putClass(key.getClass()) == keyClassId(p);
        }

        if (p != header && key.compareTo(keyAddr(p)) == 0) {
            return getSetValue(p, value);
        }

        int levelHeight = level();
        if (levelHeight > levelHeight()) {
            for (int i = levelHeight(); i < levelHeight; i++) {
                update[i] = header;
            }
            levelHeight(levelHeight);
        }

        int prefixBytes = prefixBytes(levelHeight);
        long nodeAddr = key.allocAndSerialize(prefixBytes);
        setKeyClassId(nodeAddr, OffheapClass.putClass(key.getClass()));
        setLevelHeight(nodeAddr, levelHeight);
        setValue(nodeAddr, value);

        for (int i = 0; i < levelHeight; i++) {
            setLevel(nodeAddr, i, getLevel(update[i], i));
            setLevel(update[i], i, nodeAddr);
        }

        if (getLevel(nodeAddr, 0) == 0L) {
            tail(nodeAddr);
        }

        size(size() + 1);

        return null;
    }

    public boolean isEmpty()
    {
        return 0 == size();
    }

    public K firstKey()
    {
        long addr = getLevel(header, 0);
        if (0L == addr) {
            throw new NoSuchElementException();
        }
        return prototypeKey(addr).deserialize(keyAddr(addr));
    }

    public K lastKey()
    {
        if (0L == tail() || header == tail()) {
            throw new NoSuchElementException();
        }
        return prototypeKey(tail()).deserialize(keyAddr(tail()));
    }

//    private K deserializeKey(long nodeAddr)
//    {
//        if (0L == nodeAddr) {
//            throw new NoSuchElementException();
//        }
//        return prototypeKey(nodeAddr).deserialize(keyAddr(nodeAddr));
//    }

//    public SubMap subMap(Comparable fromKey, boolean fromInclusive, Comparable toKey, boolean toInclusive)
//    {
//        ComparableOffheap fromKeyOffheap = Offheap.offheaplize(fromKey);
//        ComparableOffheap toKeyOffheap = Offheap.offheaplize(toKey);
//
//        return subMap(fromKeyOffheap, fromInclusive, toKeyOffheap, toInclusive);
//    }

    public SubMap subMap(ComparableOffheap fromKey, boolean fromInclusive, ComparableOffheap toKey, boolean toInclusive)
    {
        if (fromKey == null || toKey == null) {
            throw new NullPointerException();
        }
        if (fromKey.compareTo(toKey) > 0) {
            throw new IllegalArgumentException("fromKey > toKey");
        }
        if (isEmpty()) {
            throw new IllegalStateException("no sub map for an empty map");
        }
        if (fromKey.compareTo(keyAddr(getLevel(header, 0))) < 0) {
            throw new IllegalArgumentException("fromKey < firstKey");
        }
        if (toKey.compareTo(keyAddr(tail())) > 0) {
            throw new IllegalArgumentException("toKey > lastKey");
        }

        return new SubMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    private final class EntryIterator implements Iterator<Entry<K, V>>
    {
        long next;
        int expectedSize;
        long from;
        long to;

        EntryIterator(long from, long to)
        {
            expectedSize = size();
            this.from = from;
            this.to = to;
            next = from;
        }

        @Override
        public boolean hasNext()
        {
            return next != to;
        }

        @Override
        public Entry next()
        {
            long e = next;
            if (size() != expectedSize) {
                throw new ConcurrentModificationException();
            }
            if (e == 0L || e == to) {
                throw new NoSuchElementException();
            }

            K key = prototypeKey(next).deserialize(keyAddr(next));
            V value = value(next);
            next = getLevel(next, 0);
            return new Entry(key, value);
        }
    }

    public final class SubMap implements Iterable<Entry<K, V>>
    {
        private final ComparableOffheap fromKey;
        private final ComparableOffheap toKey;
        private final boolean fromInclusive;
        private final boolean toInclusive;

        SubMap(ComparableOffheap fromKey, boolean fromInclusive, ComparableOffheap toKey, boolean toInclusive)
        {
            this.fromKey = fromKey;
            this.fromInclusive = fromInclusive;
            this.toKey = toKey;
            this.toInclusive = toInclusive;
        }

        @Override
        public Iterator<Entry<K, V>> iterator()
        {
            long update[] = locate(fromKey, false);
            assert 0L != update[0];
            assert fromKey.compareTo(keyAddr(update[0])) >= 0;
            long from = fromInclusive && fromKey.compareTo(keyAddr(update[0])) == 0 ? update[0] : getLevel(update[0], 0);
            update = locate(toKey, false);
            assert 0L != update[0];
            assert toKey.compareTo(keyAddr(update[0])) >= 0;
            long to = toInclusive || toKey.compareTo(keyAddr(update[0])) > 0 ? getLevel(update[0], 0) : update[0];
            return new EntryIterator(from, to);
        }
    }
}
