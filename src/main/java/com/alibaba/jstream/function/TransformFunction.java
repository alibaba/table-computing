package com.alibaba.jstream.function;

import com.alibaba.jstream.table.Row;

import java.util.List;

public interface TransformFunction {
    List<Comparable[]> returnMultiRow(Row row);
}
