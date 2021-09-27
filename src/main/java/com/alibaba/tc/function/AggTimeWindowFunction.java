package com.alibaba.tc.function;

import com.alibaba.tc.table.Row;

import java.util.List;

public interface AggTimeWindowFunction {
    // 窗口区间： [windowStart, windowEnd)
    Comparable[] agg(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd);
}
