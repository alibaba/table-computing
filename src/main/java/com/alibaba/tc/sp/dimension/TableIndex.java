package com.alibaba.tc.sp.dimension;

import com.alibaba.tc.table.Index;
import com.alibaba.tc.table.Row;
import com.alibaba.tc.table.RowByTable;
import com.alibaba.tc.table.Table;

import java.util.List;

public class TableIndex {
    private final Table table;
    private final Index index;

    protected TableIndex(Table table, Index index) {
        this.table = table;
        this.index = index;
    }

    public Table getTable() {
        return table;
    }

    public List<Integer> getRows(Comparable... primaryKey) {
        return index.get(primaryKey);
    }

    public Row getRow(Comparable... primaryKey) {
        List<Integer> rows = getRows(primaryKey);
        if (null == rows || rows.size() < 1) {
            return null;
        }
        return new RowByTable(table, rows.get(0));
    }
}
