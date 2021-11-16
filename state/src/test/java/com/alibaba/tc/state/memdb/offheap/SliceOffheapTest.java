package com.alibaba.tc.state.memdb.offheap;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

public class SliceOffheapTest
{
    @Test
    public void test()
    {
        Slice slice = Slices.allocateDirect(3);
        slice.setBytes(0, Slices.utf8Slice("aaa"));
        SliceOffheap sliceOffheap = new SliceOffheap(slice);
        try {
            new SliceOffheap(Slices.utf8Slice("aaa"));
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "slice should be off-heap");
        }

        slice = Slices.allocateDirect(3);
        slice.setBytes(0, Slices.utf8Slice("aaa"));
        assertEquals(sliceOffheap.compareTo(new SliceOffheap(slice)), 0);


    }
}