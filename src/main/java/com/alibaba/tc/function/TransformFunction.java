package com.alibaba.tc.function;

import com.alibaba.tc.table.Row;

import java.util.List;

public interface TransformFunction {
    List<Comparable[]> returnMultiRow(Row row);
}
