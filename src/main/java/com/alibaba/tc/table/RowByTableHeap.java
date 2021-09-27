package com.alibaba.tc.table;

import com.alibaba.tc.exception.ColumnNotExistsException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowByTableHeap extends AbstractRow {
    private final LinkedHashMap<String, Integer> columnName2Index;
    private final List<List<Comparable>> table;
    private final int row;

    public RowByTableHeap(LinkedHashMap<String, Integer> columnName2Index, List<List<Comparable>> table, int row) {
        this.columnName2Index = requireNonNull(columnName2Index);
        this.table = table;
        this.row = row;
    }

    @Override
    public Set<String> getColumnNames() {
        return columnName2Index.keySet();
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
        return ifStr(table.get(index).get(row));
    }

    @Override
    public Comparable get(String columnName) {
        Integer index = columnName2Index.get(columnName);
        if (null == index) {
            throw new ColumnNotExistsException(format("column '%s' not exists", columnName));
        }
        return get(index);
    }

    @Override
    public int size() {
        return columnName2Index.size();
    }
}
