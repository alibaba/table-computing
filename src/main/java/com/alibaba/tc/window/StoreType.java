package com.alibaba.tc.window;

public enum StoreType {
    //默认值。适用于对窗口内的数据进行聚合计算较多的场景
    STORE_BY_COLUMN,
    //适用于对窗口内的数据进行扫描较多的场景
    STORE_BY_ROW,
    //适用于partition出的窗口数量较少而窗口内的数据量较多的场景，反之使用该存储类型可能会出现大量小片的堆外内存导致gc不到的概率增加从而造成堆外内存泄露
    STORE_ON_OFFHEAP;
}
