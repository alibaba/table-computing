package com.alibaba.jstream.sp.output;

import com.alibaba.jstream.table.Table;

public interface OutputTable {
    void stop();
    void produce(Table table) throws InterruptedException;
}
