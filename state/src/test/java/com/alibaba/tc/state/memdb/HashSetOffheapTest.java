package com.alibaba.tc.state.memdb;

import com.alibaba.sdb.plugin.offheap.HashSetOffheap;
import com.alibaba.sdb.spi.block.InternalUnsafe;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class HashSetOffheapTest
{
    @Test
    public void test()
    {
        HashSetOffheap<NsdbIndex.PagePosition> hashSetOffheap = new HashSetOffheap<>(NsdbIndex.PagePosition.class);
        hashSetOffheap.add(new NsdbIndex.PagePosition(1, 2));
        hashSetOffheap.add(new NsdbIndex.PagePosition(1, 1));
        hashSetOffheap.add(new NsdbIndex.PagePosition(1, 2));
        assertEquals(hashSetOffheap.size(), 2);

        for (NsdbIndex.PagePosition pagePosition : hashSetOffheap) {
            assertEquals(pagePosition.getPosition(), 1);
            break;
        }

        for (int i = 0; i < 16; i++) {
            hashSetOffheap.add(new NsdbIndex.PagePosition(2, i));
        }
        assertEquals(hashSetOffheap.size(), 18);

        for (int i = 0; i < 16; i++) {
            assertEquals(hashSetOffheap.remove(new NsdbIndex.PagePosition(2, i)), true);
        }
        assertEquals(hashSetOffheap.size(), 2);
        assertEquals(hashSetOffheap.isEmpty(), false);

        hashSetOffheap.remove(new NsdbIndex.PagePosition(1, 2));
        hashSetOffheap.remove(new NsdbIndex.PagePosition(1, 1));
        assertEquals(hashSetOffheap.isEmpty(), true);

        assertEquals(InternalUnsafe.getUsedMemory(), 308L);

        long total = 50_000_000L;
        long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            hashSetOffheap.add(new NsdbIndex.PagePosition(2, i));
        }
        long end = System.currentTimeMillis();
        System.out.println("add qps: " + total/((end - start)/1000.0));
        System.out.println("add elapse: " + ((end - start)/1000.0));

        System.out.println(" memory used: " + InternalUnsafe.getUsedMemory());

        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            assertEquals(hashSetOffheap.contains(new NsdbIndex.PagePosition(2, i)), true);
        }
        end = System.currentTimeMillis();
        System.out.println("contains qps: " + total/((end - start)/1000.0));
        System.out.println("contains elapse: " + ((end - start)/1000.0));

        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            assertEquals(hashSetOffheap.remove(new NsdbIndex.PagePosition(2, i)), true);
        }
        end = System.currentTimeMillis();
        System.out.println("remove qps: " + total/((end - start)/1000.0));
        System.out.println("remove elapse: " + ((end - start)/1000.0));

        System.out.println(" memory used: " + InternalUnsafe.getUsedMemory());

        hashSetOffheap = null;
        InternalUnsafe.fullGC();
        try {
            int i = 0;
            while (i < 10) {
                Thread.sleep(1000);
                System.out.println(i + " memory used: " + InternalUnsafe.getUsedMemory());
                i++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(InternalUnsafe.getUsedMemory(), 0L);
    }
}