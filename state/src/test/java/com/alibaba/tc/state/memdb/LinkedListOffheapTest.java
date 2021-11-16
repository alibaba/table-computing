package com.alibaba.tc.state.memdb;

import com.alibaba.sdb.plugin.offheap.LinkedListOffheap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

public class LinkedListOffheapTest
{
    @Test
    public void test()
    {
        LinkedListOffheap<NsdbIndex.PagePosition> linkedListOffheap = new LinkedListOffheap<NsdbIndex.PagePosition>(new NsdbIndex.PagePosition(-1, -1));
        NsdbIndex.PagePosition pagePosition = new NsdbIndex.PagePosition(1, 1);
        linkedListOffheap.add(pagePosition);
        pagePosition = new NsdbIndex.PagePosition(1, 2);
        linkedListOffheap.add(pagePosition);
        assertEquals(linkedListOffheap.size(), 2);

        linkedListOffheap.remove(pagePosition);
        assertEquals(linkedListOffheap.size(), 1);

        pagePosition = new NsdbIndex.PagePosition(1, 1);
        linkedListOffheap.remove(pagePosition);
        assertEquals(linkedListOffheap.isEmpty(), true);
    }
}