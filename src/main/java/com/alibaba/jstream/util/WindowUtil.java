package com.alibaba.jstream.util;

import com.alibaba.jstream.table.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static java.lang.Math.min;

public class WindowUtil {
    /**
     * 返回columnName列的topN的下标
     * @param rows
     * @param columnName
     * @param n
     * @return
     */
    public static int[] topN(List<Row> rows, String columnName, int n) {
        n = min(rows.size(), n);

        int size = 0;
        TreeMap<Comparable, List<Integer>> treeMap = new TreeMap<>();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            Comparable comparable = row.get(columnName);
            if (size < n) {
                putTreeMap(treeMap, comparable, i);
                size++;
            } else {
                Comparable leastKey = treeMap.firstKey();
                if (comparable.compareTo(leastKey) > 0) {
                    List<Integer> values = treeMap.get(leastKey);
                    values.remove(values.size() - 1);
                    if (values.isEmpty()) {
                        treeMap.remove(leastKey);
                    }
                    putTreeMap(treeMap, comparable, i);
                }
            }
        }

        int[] ret = new int[n];
        int i = 1;
        for (List<Integer> values : treeMap.values()) {
            for (Integer index : values) {
                ret[n - i] = index;
                i++;
            }
        }

        return ret;
    }

    private static void putTreeMap(TreeMap<Comparable, List<Integer>> treeMap, Comparable key, Integer value) {
        List<Integer> values = treeMap.get(key);
        if (null == values) {
            values = new ArrayList<>();
            treeMap.put(key, values);
        }
        values.add(value);
    }
}
