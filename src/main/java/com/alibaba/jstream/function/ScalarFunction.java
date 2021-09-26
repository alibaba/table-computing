package com.alibaba.jstream.function;

import com.alibaba.jstream.table.Row;

public interface ScalarFunction {
    Comparable[] returnOneRow(Row row) throws InterruptedException;
}
