package com.alibaba.tc.sp;

public interface Compute {
    void compute(int myThreadIndex) throws InterruptedException;
}
