package com.alibaba.tc.offheap;

import com.alibaba.tc.table.VarbyteColumn;
import org.junit.Test;

import static org.junit.Assert.*;

public class VarbyteColumnTest {

    @Test
    public void add() {
        VarbyteColumn varbyteColumn = new VarbyteColumn(7);
        varbyteColumn.add(null);
        varbyteColumn.add(new ByteArray("aaa".getBytes()));
        varbyteColumn.add(new ByteArray("s中文d".getBytes()));
        varbyteColumn.add(null);
        varbyteColumn.add(new ByteArray("ccc".getBytes()));
        varbyteColumn.add(new ByteArray("".getBytes()));
        varbyteColumn.add(new ByteArray(new byte[0]));
        assertEquals(varbyteColumn.size(), 7);
        assertEquals(varbyteColumn.get(0), null);
        assertEquals(varbyteColumn.get(2), new ByteArray("s中文d".getBytes()));
        assertEquals(varbyteColumn.get(3), null);
        assertEquals(varbyteColumn.get(4), new ByteArray("ccc".getBytes()));
        assertEquals(varbyteColumn.get(5), new ByteArray("".getBytes()));
        assertEquals(varbyteColumn.get(5), new ByteArray(new byte[0]));

        varbyteColumn = new VarbyteColumn(6500);
        //test "grow lead to crash" case
        for (int i = 0; i < 6500; i++) {
            varbyteColumn.add(new ByteArray(("" + i).getBytes()));
        }

        for (int i = 0; i < 6500; i++) {
            assertEquals(varbyteColumn.get(i), new ByteArray(("" + i).getBytes()));
        }

        VarbyteColumn varbyteColumn2 = new VarbyteColumn(6500);
        for (int i = 0; i < 6500; i++) {
            varbyteColumn2.add(varbyteColumn.get(i));
        }

        for (int i = 0; i < 6500; i++) {
            assertEquals(varbyteColumn2.get(i), new ByteArray(("" + i).getBytes()));
        }

        VarbyteColumn varbyteColumn3 = new VarbyteColumn(6500);
        for (int i = 0; i < 6500; i++) {
            varbyteColumn3.add(varbyteColumn2.get(i));
        }

        for (int i = 0; i < 6500; i++) {
            assertEquals(varbyteColumn3.get(i), new ByteArray(("" + i).getBytes()));
        }
    }
}