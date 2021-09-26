package com.alibaba.jstream.function;

import com.alibaba.jstream.table.Row;

import java.util.List;

public interface OverFunction {
    //可以一次计算多个over列出来，比如count,max,avg,rank四列一次计算出来
    Comparable[] agg(List<Comparable> partitionByColumns, List<Row> rows);
}
