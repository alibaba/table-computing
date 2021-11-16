package com.alibaba.tc.state.memdb;

import com.alibaba.sdb.plugin.offheap.BooleanOffheap;
import com.alibaba.sdb.plugin.offheap.DoubleOffheap;
import com.alibaba.sdb.plugin.offheap.HashSetOffheap;
import com.alibaba.sdb.plugin.offheap.IntegerOffheap;
import com.alibaba.sdb.plugin.offheap.LongOffheap;
import com.alibaba.sdb.plugin.offheap.Offheap;
import com.alibaba.sdb.plugin.offheap.SkipListMapOffheap;
import com.alibaba.sdb.plugin.offheap.SliceOffheap;
import com.alibaba.sdb.spi.block.InternalUnsafe;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SkipListMapOffheapTest
{
    private static Object ref;

    public void testSerialize()
    {
        SkipListMapOffheap skipListForSlice = new SkipListMapOffheap();
        Slice slice = Slices.allocateDirect(3);
        slice.setBytes(0, Slices.utf8Slice("aaa"));
        HashSetOffheap<NsdbIndex.PagePosition> hashSetOffheapInner = new HashSetOffheap(NsdbIndex.PagePosition.class);
        hashSetOffheapInner.add(new NsdbIndex.PagePosition(3, 1));
        hashSetOffheapInner.add(new NsdbIndex.PagePosition(3, 1));
        skipListForSlice.put(new SliceOffheap(slice), hashSetOffheapInner);
        assertEquals(((HashSetOffheap) skipListForSlice.get(new SliceOffheap(slice))).size(), 1);

        slice = Slices.allocateDirect(3);
        slice.setBytes(0, Slices.utf8Slice("ccc"));
        SkipListMapOffheap skipListMapOffheapInner = new SkipListMapOffheap();
        skipListMapOffheapInner.put(new BooleanOffheap(false), new DoubleOffheap(1.1));
        skipListMapOffheapInner.put(new BooleanOffheap(true), new DoubleOffheap(2.1));
        skipListMapOffheapInner.put(new BooleanOffheap(false), new DoubleOffheap(3.1));
        skipListForSlice.put(Offheap.offheaplize(slice, null), skipListMapOffheapInner);

        slice = Slices.allocateDirect(3);
        slice.setBytes(0, Slices.utf8Slice("bbb"));
        skipListForSlice.put(Offheap.offheaplize(slice, null), new LongOffheap(8L));

        for (Object object : skipListForSlice) {
            SkipListMapOffheap.Entry entry = (SkipListMapOffheap.Entry) object;
            System.out.println("key: " + entry.getKey());
        }

        System.out.println("memory used: " + InternalUnsafe.getUsedMemory());

        int total = 1_00_000;
        for (int i = 0; i < total; i++) {
            skipListForSlice.put(new SliceOffheap(Slices.utf8Slice("ccc" + i)), new LongOffheap(8L));
        }

        SkipListMapOffheap.SubMap subMap = skipListForSlice.subMap(new SliceOffheap(Slices.utf8Slice("bbb")),
                false,
                new SliceOffheap(Slices.utf8Slice("ccc1")),
                true);
        for (Object object : subMap) {
            SkipListMapOffheap.Entry entry = (SkipListMapOffheap.Entry) object;
            System.out.println("key: " + entry.getKey());
        }

        assertEquals(skipListForSlice.size(), total + 3);

        free();
    }

    @Test
    public void test()
    {
        testSerialize();

        SkipListMapOffheap<IntegerOffheap, NsdbIndex.PagePosition> skipListMapOffheap = new SkipListMapOffheap<>();
        assert null == skipListMapOffheap.put(new IntegerOffheap(2), new NsdbIndex.PagePosition(1, 2));
        assert null == skipListMapOffheap.put(new IntegerOffheap(3), new NsdbIndex.PagePosition(1, 2));
        assert null == skipListMapOffheap.put(new IntegerOffheap(1), new NsdbIndex.PagePosition(1, 2));
        assert null == skipListMapOffheap.put(new IntegerOffheap(4), new NsdbIndex.PagePosition(3, 2));
        NsdbIndex.PagePosition pagePosition = skipListMapOffheap.put(new IntegerOffheap(4), new NsdbIndex.PagePosition(2, 2));
        assert null != pagePosition;
        assert pagePosition.getPage() == 3;
        assertEquals(skipListMapOffheap.size(), 4);
        assertEquals(skipListMapOffheap.firstKey().compareTo(new IntegerOffheap(1)), 0);
        assertEquals(skipListMapOffheap.lastKey().compareTo(new IntegerOffheap(4)), 0);

        int i = 1;
        for (SkipListMapOffheap.Entry<IntegerOffheap, NsdbIndex.PagePosition> entry : skipListMapOffheap) {
            assertEquals(entry.getKey().compareTo(new IntegerOffheap(i)), 0);
            if (4 == i) {
                assertEquals(entry.getValue().getPage(), 2);
            }
            i++;
        }

        for (i = 3; i < 18; i++) {
            skipListMapOffheap.put(new IntegerOffheap(i), new NsdbIndex.PagePosition(4, i));
        }
        assertEquals(skipListMapOffheap.size(), 17);

        for (i = 3; i < 18; i++) {
            assertEquals(skipListMapOffheap.remove(new IntegerOffheap(i)) != null, true);
        }
        assertEquals(skipListMapOffheap.size(), 2);
        assertEquals(skipListMapOffheap.isEmpty(), false);

        skipListMapOffheap.remove(new IntegerOffheap(1));
        skipListMapOffheap.remove(new IntegerOffheap(2));
        assertEquals(skipListMapOffheap.isEmpty(), true);

        skipListMapOffheap.put(null, null);
        assertEquals(skipListMapOffheap.get(null), null);
        assertEquals(skipListMapOffheap.size(), 1);

        skipListMapOffheap.put(null, new NsdbIndex.PagePosition(1, 2));
        assertEquals(skipListMapOffheap.get(null), new NsdbIndex.PagePosition(1, 2));
        assertEquals(skipListMapOffheap.size(), 1);

        skipListMapOffheap.put(new IntegerOffheap(1), null);
        assertEquals(skipListMapOffheap.get(new IntegerOffheap(1)), null);
        assertEquals(skipListMapOffheap.size(), 2);

        assertEquals(skipListMapOffheap.remove(null), new NsdbIndex.PagePosition(1, 2));
        assertEquals(skipListMapOffheap.remove(new IntegerOffheap(1)), null);
        assertEquals(skipListMapOffheap.isEmpty(), true);

        //header的大小 + nullKeyNode的大小 + 两个前缀的long size（见InternalUnsafe.alloc）+ 1个前缀的long size和1个refCount的long size
//        assertEquals(InternalUnsafe.getUsedMemory(), 2 * (Byte.BYTES + 16 * Long.BYTES + Long.BYTES + Integer.BYTES) + 2 * Long.BYTES + Long.BYTES + Long.BYTES);

        long total = 5_000_000L;
        long start = System.currentTimeMillis();
        for (i = 0; i < total; i++) {
            skipListMapOffheap.put(new IntegerOffheap(i), new NsdbIndex.PagePosition(i, 2));
        }
        long end = System.currentTimeMillis();
        System.out.println("put qps: " + total/((end - start)/1000.0));
        System.out.println("put elapse: " + ((end - start)/1000.0));

        System.out.println("memory used: " + InternalUnsafe.getUsedMemory());

        SkipListMapOffheap<IntegerOffheap, NsdbIndex.PagePosition>.SubMap subMap = skipListMapOffheap.subMap(new IntegerOffheap(100), true, new IntegerOffheap(200), false);
        i = 100;
        for (SkipListMapOffheap.Entry<IntegerOffheap, NsdbIndex.PagePosition> entry : subMap) {
            assertEquals(entry.getKey().compareTo(new IntegerOffheap(i)), 0);
            i++;
        }
        assertEquals(i, 200);

        start = System.currentTimeMillis();
        for (i = 0; i < total; i++) {
            assertEquals(skipListMapOffheap.get(new IntegerOffheap(i)), new NsdbIndex.PagePosition(i, 2));
        }
        end = System.currentTimeMillis();
        System.out.println("get qps: " + total/((end - start)/1000.0));
        System.out.println("get elapse: " + ((end - start)/1000.0));

        start = System.currentTimeMillis();
        subMap = skipListMapOffheap.subMap(new IntegerOffheap(0), true, new IntegerOffheap((int) total-1), true);
        i = 0;
        for (SkipListMapOffheap.Entry<IntegerOffheap, NsdbIndex.PagePosition> entry : subMap) {
            assertEquals(entry.getKey().compareTo(new IntegerOffheap(i)), 0);
            i++;
        }
        end = System.currentTimeMillis();
        System.out.println("subMap qps: " + total/((end - start)/1000.0));
        System.out.println("subMap elapse: " + ((end - start)/1000.0));
        assertEquals(i, total);

        start = System.currentTimeMillis();
        for (i = 0; i < total; i++) {
            skipListMapOffheap.remove(new IntegerOffheap(i));
        }
        end = System.currentTimeMillis();
        System.out.println("remove qps: " + total/((end - start)/1000.0));
        System.out.println("remove elapse: " + ((end - start)/1000.0));

        System.out.println("memory used: " + InternalUnsafe.getUsedMemory());

        start = System.currentTimeMillis();
        for (i = 0; i < total; i++) {
            skipListMapOffheap.put(new IntegerOffheap(i), new NsdbIndex.PagePosition(1, 2));
        }
        end = System.currentTimeMillis();
        System.out.println("put qps: " + total/((end - start)/1000.0));
        System.out.println("put elapse: " + ((end - start)/1000.0));

        System.out.println("memory used: " + InternalUnsafe.getUsedMemory());

        ref = subMap;
        skipListMapOffheap = null;
        InternalUnsafe.fullGC();
        try {
            i = 0;
            while (i < 3) {
                Thread.sleep(1000);
                System.out.println(i + " memory used: " + InternalUnsafe.getUsedMemory());
                i++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(InternalUnsafe.getUsedMemory() > 0, true);

        ref = null;
        subMap = null;

        free();
    }

    void free()
    {
        System.out.println("before free memory used: " + InternalUnsafe.getUsedMemory() + " direct memory used: " + InternalUnsafe.directMemoryUsed());

        InternalUnsafe.fullGC();
        try {
            int i = 0;
            while (i < 10) {
                Thread.sleep(1000);
                InternalUnsafe.fullGC();
                System.out.println(i + " memory used: " + InternalUnsafe.getUsedMemory() + " direct memory used: " + InternalUnsafe.directMemoryUsed());
                i++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(InternalUnsafe.getUsedMemory(), 0L);
        assertEquals(InternalUnsafe.directMemoryUsed(), 0L);
    }
}