package com.alibaba.tc.function;

import com.alibaba.tc.table.Row;

import java.util.List;

public interface WindowFunction {
    //可以一次计算每一行的多个列出来，比如count,max,avg,rank四列一次计算出来
    List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows, int needCompute);
}
