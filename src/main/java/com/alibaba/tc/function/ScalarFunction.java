package com.alibaba.tc.function;

import com.alibaba.tc.table.Row;

public interface ScalarFunction {
    Comparable[] returnOneRow(Row row);
}
