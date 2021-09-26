package com.alibaba.jstream.table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

public class SortedTable {
    private TreeMap<List<Comparable>, LinkedList<List<Comparable>>> sorted = new TreeMap<>(new Comparator<List<Comparable>>() {
        @Override
        public int compare(List<Comparable> o1, List<Comparable> o2) {
            if (o1 == o2) {
                return 0;
            }
            if (o1.size() != o2.size()) {
                throw new IllegalArgumentException();
            }
            for (int i = 0; i < o1.size(); i++) {
                int cmp = o1.get(i).compareTo(o2.get(i));
                if (cmp == 0) {
                    continue;
                }
                return cmp;
            }
            return 0;
        }
    });
    private final LinkedHashMap<String, Integer> columnName2Index = new LinkedHashMap<>();
    private final String[] sortColumnNames;
    private int size;

    public SortedTable(Table table, String... sortColumnNames) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            columnName2Index.put(table.getColumn(i).name(), i);
        }

        this.sortColumnNames = requireNonNull(sortColumnNames);
    }

    public void addRow(Table table, int row) {
        List<Comparable> key = new ArrayList<>(sortColumnNames.length);
        for (int i = 0; i < sortColumnNames.length; i++) {
            key.add(table.getColumn(sortColumnNames[i]).get(row));
        }

        List<Comparable> record = new ArrayList<>(columnName2Index.keySet().size());
        for (int i = 0; i < columnName2Index.keySet().size(); i++) {
            record.add(table.getColumn(i).get(row));
        }

        LinkedList<List<Comparable>> rows = sorted.get(key);
        if (null == rows) {
            rows = new LinkedList<>();
            sorted.put(key, rows);
        }
        rows.addLast(record);

        size++;
    }

    public int countLessThan(long start) {
        List<Comparable> toKey = new ArrayList<>(1);
        toKey.add(start);

        int sum = 0;
        for (LinkedList<List<Comparable>> rowsForKey : sorted.headMap(toKey, false).values()) {
            sum += rowsForKey.size();
        }

        return sum;
    }

    public void removeLessThan(long start) {
        List<Comparable> toKey = new ArrayList<>(1);
        toKey.add(start);
        NavigableMap<List<Comparable>, LinkedList<List<Comparable>>> willRemove = sorted.headMap(toKey, false);

        int remove = 0;
        for (LinkedList<List<Comparable>> rowsForKey : willRemove.values()) {
            remove += rowsForKey.size();
        }

        willRemove.clear();

        size -= remove;
    }

    public void removeFirstRow() {
        if (size < 1) {
            throw new IllegalStateException("no row");
        }
        sorted.firstEntry().getValue().removeFirst();
        if (sorted.firstEntry().getValue().size() <= 0) {
            sorted.remove(sorted.firstKey());
        }
        size--;
    }

    public int size() {
        return size;
    }

    public List<Row> rows() {
        List<Row> rows = new ArrayList<>(size);
        for (LinkedList<List<Comparable>> rowsForKey : sorted.values()) {
            for (List<Comparable> row : rowsForKey) {
                rows.add(new RowByList(columnName2Index, row));
            }
        }

        return rows;
    }

    public void clear() {
        sorted.clear();
        size = 0;
    }

    public List<Comparable> firstKey() {
        return sorted.firstKey();
    }

    public List<Comparable> lastKey() {
        return sorted.lastKey();
    }
}
