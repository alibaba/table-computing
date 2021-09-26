package com.alibaba.jstream.table;

import com.alibaba.jstream.exception.ColumnNotExistsException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowByList extends AbstractRow {
    private final LinkedHashMap<String, Integer> columnName2Index;
    private final List<Comparable> row;

    public RowByList(LinkedHashMap<String, Integer> columnName2Index, List<Comparable> row) {
        this.columnName2Index = requireNonNull(columnName2Index);
        this.row = requireNonNull(row);
    }

    @Override
    public Set<String> getColumnNames() {
        return columnName2Index.keySet();
    }

    @Override
    public Comparable[] getAll() {
        return row.toArray(new Comparable[0]);
    }

    @Override
    public Comparable get(int index) {
        return ifStr(row.get(index));
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
