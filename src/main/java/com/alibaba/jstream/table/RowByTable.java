package com.alibaba.jstream.table;

import com.alibaba.jstream.exception.ColumnNotExistsException;

import java.util.Set;

import static java.lang.String.format;

public class RowByTable extends AbstractRow {
    private final Table table;
    private final int row;

    public RowByTable(Table table, int row) {
        this.table = table;
        this.row = row;
    }

    @Override
    public Set<String> getColumnNames() {
        return table.getColumnIndex().keySet();
    }

    @Override
    public Comparable[] getAll() {
        int len = size();
        Comparable[] comparables = new Comparable[len];
        for (int i = 0; i < len; i++) {
            comparables[i] = get(i);
        }

        return comparables;
    }

    @Override
    public Comparable get(int index) {
        return ifStr(table.getColumn(index).get(row));
    }

    @Override
    public Comparable get(String columnName) {
        Integer index = table.getIndex(columnName);
        if (null == index) {
            throw new ColumnNotExistsException(format("column '%s' not exists", columnName));
        }
        return get(index);
    }

    @Override
    public int size() {
        return table.getColumns().size();
    }
}
