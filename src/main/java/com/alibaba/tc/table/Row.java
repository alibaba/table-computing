package com.alibaba.tc.table;

import com.alibaba.tc.offheap.ByteArray;

import java.util.Set;

public interface Row {
    /**
     * ByteArray to String to avoid unaware use ByteArray as String
     * @param comparable    input value
     * @return              if input value is ByteArray return String else return input value
     */
    default Comparable ifStr(Comparable comparable) {
        if (null != comparable && comparable instanceof ByteArray) {
            return comparable.toString();
        }
        return comparable;
    }

    /**
     *
     * @return LinkedHashMap.LinkedKeySet 保证列的顺序
     */
    Set<String> getColumnNames();
    Comparable[] getAll();
    Comparable get(int index);
    Comparable get(String columnName);
    String getString(String columnName);
    Double getDouble(String columnName);
    Long getLong(String columnName);
    Integer getInteger(String columnName);
    String getString(int index);
    Double getDouble(int index);
    Long getLong(int index);
    Integer getInteger(int index);
    int size();
}
