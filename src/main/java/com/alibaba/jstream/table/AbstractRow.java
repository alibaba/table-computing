package com.alibaba.jstream.table;

import static com.alibaba.jstream.util.ScalarUtil.toDouble;
import static com.alibaba.jstream.util.ScalarUtil.toInteger;
import static com.alibaba.jstream.util.ScalarUtil.toLong;
import static com.alibaba.jstream.util.ScalarUtil.toStr;

public abstract class AbstractRow implements Row {
    @Override
    public String getString(String columnName) {
        return toStr(get(columnName));
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
