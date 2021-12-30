package com.alibaba.tc.table;

import java.math.BigDecimal;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;

public class BigDecimalColumn implements ColumnInterface<BigDecimal> {
    private VarbyteColumn varbyteColumn;

    BigDecimalColumn() {
        varbyteColumn = new VarbyteColumn();
    }

    public BigDecimalColumn(int capacity) {
        varbyteColumn = new VarbyteColumn(capacity);
    }

    @Override
    public long serializeSize() {
        return varbyteColumn.serializeSize();
    }

    @Override
    public void serialize(byte[] bytes, long offset, long length) {
        varbyteColumn.serialize(bytes, offset, length);
    }

    @Override
    public void deserialize(byte[] bytes, long offset, long length) {
        varbyteColumn.deserialize(bytes, offset, length);
    }

    @Override
    public void add(BigDecimal bigDecimal) {
        if (null == bigDecimal) {
            varbyteColumn.add(null);
        } else {
            varbyteColumn.add(bigDecimal.toString());
        }
    }

    @Override
    public BigDecimal get(int index) {
        byte[] bytes = varbyteColumn.getBytes(index);
        if (null == bytes) {
            return null;
        } else {
            return new BigDecimal(new String(bytes));
        }
    }

    @Override
    public long size() {
        return varbyteColumn.size();
    }
}
