package com.alibaba.tc.table;

import java.math.BigDecimal;

import static com.alibaba.tc.util.ScalarUtil.*;

public abstract class AbstractRow implements Row {
    @Override
    public String getString(String columnName) {
        return toStr(get(columnName));
    }

    @Override
    public BigDecimal getBigDecimal(String columnName) {
        return toBigDecimal(get(columnName));
    }

    @Override
    public Double getDouble(String columnName) {
        return toDouble(get(columnName));
    }

    @Override
    public Long getLong(String columnName) {
        return toLong(get(columnName));
    }

    @Override
    public Integer getInteger(String columnName) {
        return toInteger(get(columnName));
    }

    @Override
    public String getString(int index) {
        return toStr(get(index));
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return toBigDecimal(get(index));
    }

    @Override
    public Double getDouble(int index) {
        return toDouble(get(index));
    }

    @Override
    public Long getLong(int index) {
        return toLong(get(index));
    }

    @Override
    public Integer getInteger(int index){
        return toInteger(get(index));
    }
}
