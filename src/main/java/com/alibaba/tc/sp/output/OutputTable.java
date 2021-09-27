package com.alibaba.tc.sp.output;

import com.alibaba.tc.table.Table;

public interface OutputTable {
    void stop();
    void produce(Table table) throws InterruptedException;
}
