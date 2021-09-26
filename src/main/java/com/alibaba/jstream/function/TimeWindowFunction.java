package com.alibaba.jstream.function;

import com.alibaba.jstream.table.Row;

import java.util.List;

public interface TimeWindowFunction {
    // 可以一次计算每一行的多个列出来，比如count,max,avg,rank四列一次计算出来
    // 窗口区间： [windowStart, windowEnd)
    List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd);
}
