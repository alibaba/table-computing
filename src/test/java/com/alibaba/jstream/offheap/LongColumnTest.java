package com.alibaba.jstream.offheap;

import com.alibaba.jstream.table.LongColumn;
import org.junit.Test;

import static org.junit.Assert.*;

public class LongColumnTest {
    @Test
    public void add() {
        LongColumn longColumn = new LongColumn(7);
        longColumn.add(null);
        longColumn.add(1L);
        longColumn.add(2L);
        longColumn.add(3L);
        longColumn.add(null);
        longColumn.add(5L);
        longColumn.add(0L);
        assertEquals(longColumn.get(0), null);
        assert longColumn.get(2) == 2L;
        assertEquals(longColumn.get(4), null);
        assert longColumn.get(5) == 5L;
        assert longColumn.get(6) == 0L;
        assertEquals(longColumn.size(), 7);

        longColumn = new LongColumn(6500);
        //test "grow lead to crash" case
        for (int i = 0; i < 6500; i++) {
            longColumn.add((long) i);
        }
        for (int i = 0; i < 6500; i++) {
            assert longColumn.get(i) == (long) i;
        }
    }
}