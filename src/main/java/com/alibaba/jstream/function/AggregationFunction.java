package com.alibaba.jstream.function;

import com.alibaba.jstream.table.Row;

import java.util.List;

public interface AggregationFunction {
    Comparable[] agg(List<Comparable> groupByColumns, List<Row> rows);
}
