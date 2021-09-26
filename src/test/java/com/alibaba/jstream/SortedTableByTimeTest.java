package com.alibaba.jstream;

import com.alibaba.jstream.table.Column;
import com.alibaba.jstream.table.ColumnTypeBuilder;
import com.alibaba.jstream.table.SlideTableOffheap;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.TableBuilder;
import com.alibaba.jstream.table.Type;
import org.junit.Test;

public class SortedTableByTimeTest {

    @Test
    public void removeLessThan() {
        TableBuilder tableBuilder = new TableBuilder(new ColumnTypeBuilder().column("ts", Type.BIGINT).build());
        Column<Long> column = tableBuilder.getColumn("ts");
        column.add(0L);
        column.add(1L);
        column.add(2L);
        column.add(3L);
        column.add(5L);
        column.add(6L);
        column.add(8L);
        column.add(8L);
        column.add(8L);
        Table table = tableBuilder.build();
        SlideTableOffheap sortedTableByTime = new SlideTableOffheap(table,"ts");
        sortedTableByTime.addRow(table, 0);
        sortedTableByTime.addRow(table, 1);
        sortedTableByTime.addRow(table, 2);
        sortedTableByTime.addRow(table, 3);
        sortedTableByTime.addRow(table, 4);
        sortedTableByTime.addRow(table, 5);
        sortedTableByTime.addRow(table, 6);
        sortedTableByTime.addRow(table, 7);
        sortedTableByTime.addRow(table, 8);

        assert sortedTableByTime.countLessThan(0) == 0;
        assert sortedTableByTime.countLessThan(1) == 1;
        assert sortedTableByTime.countLessThan(4) == 4;
        assert sortedTableByTime.countLessThan(7) == 6;
        assert sortedTableByTime.countLessThan(8) == 6;
        assert sortedTableByTime.countLessThan(4534) == 9;

        sortedTableByTime.removeLessThan(0);
        assert sortedTableByTime.size() == 9;

        sortedTableByTime.removeLessThan(5);
        assert sortedTableByTime.tableSize() == 9;
        assert sortedTableByTime.rows().get(0).getLong("ts") == 5L;

        sortedTableByTime.removeLessThan(6);
        assert sortedTableByTime.tableSize() == 4;
        assert sortedTableByTime.rows().get(0).getLong("ts") == 6L;

        sortedTableByTime.removeLessThan(8);
        assert sortedTableByTime.size() == 3;
        assert sortedTableByTime.tableSize() == 4;
    }
}