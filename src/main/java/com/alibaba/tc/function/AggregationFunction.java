package com.alibaba.tc.function;

import com.alibaba.tc.table.Row;

import java.util.List;

public interface AggregationFunction {
    Comparable[] agg(List<Comparable> groupByColumns, List<Row> rows);
}
