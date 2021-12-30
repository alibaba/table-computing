package com.alibaba.tc.table;

import com.alibaba.tc.exception.UnknownTypeException;
import com.alibaba.tc.offheap.ByteArray;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.JDBCType;

public enum Type {
    VARCHAR,
    INT,
    BIGINT,
    DOUBLE,
    BIGDECIMAL;

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
        if (clazz == BigDecimal.class) {
            return BIGDECIMAL;
        }

        throw new UnknownTypeException(object.getClass().getName());
    }

    public static JDBCType toJDBCType(Type type) {
        switch (type) {
            case VARCHAR:
            case BIGDECIMAL:
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
