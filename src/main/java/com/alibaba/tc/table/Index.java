package com.alibaba.tc.table;

import com.alibaba.tc.offheap.ByteArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Index {
    private final Map<List<Comparable>, List<Integer>> columns2Rows;

    public Index() {
        columns2Rows = new HashMap<>();
    }

    public void put(List<Comparable> columns, int row) {
        List<Integer> rows = columns2Rows.get(columns);
        if (null == rows) {
            columns2Rows.put(columns, new ArrayList<Integer>(1){{
                add(row);
            }});
        } else {
            rows.add(row);
        }
    }

    public Map<List<Comparable>, List<Integer>> getColumns2Rows() {
        return columns2Rows;
    }

    public List<Integer> get(List<Comparable> key) {
        return columns2Rows.get(key);
    }

    public List<Integer> get(Comparable[] key) {
        List<Comparable> keyList = new ArrayList<>(key.length);
        for (Comparable elem : key) {
            if (elem instanceof String) {
                keyList.add(new ByteArray((String) elem));
            } else {
                keyList.add(elem);
            }
        }
        return columns2Rows.get(keyList);
    }
}
