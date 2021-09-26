package com.alibaba.jstream.function;

import com.alibaba.jstream.table.Row;

import java.util.List;

public interface AggTimeWindowFunction {
    // 窗口区间： [windowStart, windowEnd)
    Comparable[] agg(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd);
}
