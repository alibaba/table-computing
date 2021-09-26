package com.alibaba.jstream.table;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;

public class DoubleColumn implements ColumnInterface<Double> {
    private LongColumn longColumn;

    DoubleColumn() {
        longColumn = new LongColumn();
    }

    public DoubleColumn(int capacity) {
        longColumn = new LongColumn(capacity);
    }

    @Override
    public long serializeSize() {
        return longColumn.serializeSize();
    }

    @Override
    public void serialize(byte[] bytes, long offset, long length) {
        longColumn.serialize(bytes, offset, length);
    }

    @Override
    public void deserialize(byte[] bytes, long offset, long length) {
        longColumn.deserialize(bytes, offset, length);
    }

    @Override
    public void add(Double d) {
        if (null == d) {
            longColumn.add(null);
        } else {
            longColumn.add(doubleToLongBits(d));
        }
    }

    @Override
    public Double get(int index) {
        Long l = longColumn.get(index);
        if (null == l) {
            return null;
        } else {
            return longBitsToDouble(l);
        }
    }

    @Override
    public long size() {
        return longColumn.size();
    }
}
