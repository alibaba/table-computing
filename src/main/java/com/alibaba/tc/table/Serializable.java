package com.alibaba.tc.table;

public interface Serializable {
    long serializeSize();
    void serialize(byte[] bytes, long offset, long length);
    void deserialize(byte[] bytes, long offset, long length);
}
