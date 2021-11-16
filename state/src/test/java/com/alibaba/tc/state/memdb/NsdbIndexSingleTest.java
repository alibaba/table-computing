package com.alibaba.tc.state.memdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.lang.Integer.min;
import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

public class NsdbIndexSingleTest {
    private int compareComparable(Comparable cmp1, Comparable cmp2) {
        if (cmp1 == null && cmp2 == null) {
            return 0;
        }
        if (cmp1 == null && cmp2 != null) {
            return 1;
        }
        if (cmp1 != null && cmp2 == null) {
            return -1;
        }

        return cmp1.compareTo(cmp2);
    }

    @org.testng.annotations.Test
    public void testTreeMap() {
        TreeMap<List<Comparable>, String> indexTree = new TreeMap<>(new Comparator<List<Comparable>>() {
            /**
             * 长度为0的Slice最小，null最大
             */
            @Override
            public int compare(List<Comparable> e1,
                               List<Comparable> e2) {
                int min = min(e1.size(), e2.size());
                for (int i = 0; i < min; i++) {
                    Comparable cmp1 = e1.get(i);
                    Comparable cmp2 = e2.get(i);

                    int cmp = compareComparable(cmp1, cmp2);

                    if (0 != cmp) {
                        return cmp;
                    }
                }
                return e1.size() - e2.size();
            }
        });

        List<Comparable> k1 = new ArrayList<Comparable>() {{
            add(1);
            add(2);
            add(3);
        }},
                k2 = new ArrayList<Comparable>() {{
                    add(1);
                    add(null);
                    add(3);
                }},
                k3 = new ArrayList<Comparable>() {{
                    add(1);
                    add(3);
                    add(3);
                }},
                k4 = new ArrayList<Comparable>() {{
                    add(2);
                    add(2);
                    add(3);
                }};
        indexTree.put(k1, "aaa");
        indexTree.put(k2, "bbb");
        indexTree.put(k3, "ccc");
        indexTree.put(k4, "ddd");
        NavigableMap<List<Comparable>, String> subMap = indexTree.subMap(
                new ArrayList<Comparable>() {{
                    add(1);
                }},
                true,
                new ArrayList<Comparable>() {{
                    add(2);
                }},
                true);
        List<Comparable> toKey = indexTree.comparator().compare(k4, subMap.lastKey()) > 0 ? subMap.lastKey() : k4;
        subMap = subMap.subMap(k3, true, subMap.lastKey(), false);
        assertEquals(subMap.size(), 1);
    }
}