package com.alibaba.jstream.table;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ColumnTypeBuilder {
    private final Map<String, Type> map = new LinkedHashMap<>();

    public ColumnTypeBuilder column(String column, Type type) {
        requireNonNull(column);
        requireNonNull(type);
        map.put(column, type);
        return this;
    }

    public Map<String, Type> build() {
        return map;
    }
}
