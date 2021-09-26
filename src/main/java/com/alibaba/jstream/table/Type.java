package com.alibaba.jstream.table;

import com.alibaba.jstream.exception.UnknownTypeException;
import com.alibaba.jstream.offheap.ByteArray;

import java.sql.JDBCType;

public enum Type {
    VARCHAR,
    INT,
    BIGINT,
    DOUBLE;

    private static Type[] cache = values();
    public static Type valueOf(int ordinal) {
        return cache[ordinal];
    }

    public static Type getType(Object object) {
        if (null == object) {
            throw new NullPointerException();
        }

        Class clazz = object.getClass();
        if (clazz == Integer.class) {
            return INT;
        }
        if (clazz == Long.class) {
            return BIGINT;
        }
        if (clazz == Double.class) {
            return DOUBLE;
        }
        if (clazz == ByteArray.class || clazz == String.class) {
            return VARCHAR;
        }

        throw new UnknownTypeException(object.getClass().getName());
    }

    public static JDBCType toJDBCType(Type type) {
        switch (type) {
            case VARCHAR:
                return JDBCType.VARCHAR;
            case INT:
                return JDBCType.INTEGER;
            case BIGINT:
                return JDBCType.BIGINT;
            case DOUBLE:
                return JDBCType.DOUBLE;
            default:
                throw new UnknownTypeException(null == type ? "null" : type.name());
        }
    }
}
