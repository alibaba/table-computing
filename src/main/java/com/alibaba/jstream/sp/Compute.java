package com.alibaba.jstream.sp;

public interface Compute {
    void compute(int myThreadIndex) throws InterruptedException;
}
