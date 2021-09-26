package com.alibaba.jstream.sp.input;

import com.alibaba.jstream.table.Table;

public interface StreamTable {
    void start();
    void stop();
    Table consume() throws InterruptedException;
    boolean isFinished();
}
