package com.alibaba.tc.table;

import com.alibaba.tc.exception.UnknownTypeException;
import com.alibaba.tc.offheap.ByteArray;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.tc.table.Type.VARCHAR;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class TableBuilder {
    private final LinkedHashMap<String, Integer> columnName2Index = new LinkedHashMap<>();
    private final List<Column> columns;

    public TableBuilder(Map<String, Type> columnTypeMap) {
        if (columnTypeMap.size() <= 0) {
            throw new IllegalArgumentException("at least one column");
        }

        int i = 0;
        columns = new ArrayList<>(columnTypeMap.size());
        for (String columnName : columnTypeMap.keySet()) {
            columns.add(new Column(columnName, columnTypeMap.get(columnName)));
            columnName2Index.put(columnName, i++);
        }
    }

    public TableBuilder(List<Column> columns) {
        if (columns.size() <= 0) {
            throw new IllegalArgumentException("at least one column");
        }
        this.columns = columns;
    }

    public Column getColumn(String columnName) {
        return columns.get(columnName2Index.get(columnName));
    }

    public int size() {
        return columns.get(0).size();
    }

    public int columnSize() {
        return columns.size();
    }

    public Type getType(int index) {
        return columns.get(index).getType();
    }

    public TableBuilder append(int index, ByteArray value) {
        if (null == value) {
            columns.get(index).add(null);
            return this;
        }

        Type type = columns.get(index).getType();

        Comparable comparable;
        if (type != VARCHAR) {
            String string = new String(value.getBytes(), value.getOffset(), value.getLength());
            string = string.trim();
            switch (type) {
                case DOUBLE:
                    comparable = parseDouble(string);
                    break;
                case BIGINT:
                    comparable = parseLong(string);
                    break;
                case INT:
                    comparable = parseInt(string);
                    break;
                default:
                    throw new UnknownTypeException(type.name());
            }
        } else {
            comparable = value;
        }

        columns.get(index).add(comparable);
        return this;
    }

    public TableBuilder append(int index, Integer value) {
        appendValue(index, value);
        return this;
    }

    public TableBuilder append(int index, Long value) {
        appendValue(index, value);
        return this;
    }

    public TableBuilder append(int index, Double value) {
        appendValue(index, value);
        return this;
    }

    public TableBuilder append(int index, String value) {
        appendValue(index, value);
        return this;
    }

    public TableBuilder appendValue(int index, Comparable value) {
        if (null == value) {
            columns.get(index).add(null);
            return this;
        }

        columns.get(index).add(value);
        return this;
    }

    public Table build() {
        return new Table(columns);
    }
}
