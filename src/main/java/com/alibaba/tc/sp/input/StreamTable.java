package com.alibaba.tc.sp.input;

import com.alibaba.tc.table.Table;

public interface StreamTable {
    void start();
    void stop();
    Table consume() throws InterruptedException;
    boolean isFinished();
}
