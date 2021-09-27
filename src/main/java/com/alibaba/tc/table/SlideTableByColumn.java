package com.alibaba.tc.table;

import com.alibaba.tc.exception.OutOfOrderException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static java.lang.String.format;

public class SlideTableByColumn implements SlideTable {
    private List<List<Comparable>> table;
    private final LinkedHashMap<String, Integer> columnName2Index = new LinkedHashMap<>();
    private final int timeColumnIndex;
    private int start;
    private int size;

    public SlideTableByColumn(Table table, String timeColumnName) {
        int columnSize = table.getColumns().size();
        for (int i = 0; i < columnSize; i++) {
            columnName2Index.put(table.getColumn(i).name(), i);
        }
        timeColumnIndex = columnName2Index.get(timeColumnName);
    }

    private void initTable() {
        if (null == table) {
            int columnSize = columnName2Index.size();
            table = new ArrayList<>(columnSize);
            for (int i = 0; i < columnSize; i++) {
                table.add(new ArrayList<>());
            }
        }
    }

    public void addRow(Table table, int row) {
        long ts = (long) table.getColumn(timeColumnIndex).get(row);
        if (size > 0) {
            long pre = (long) this.table.get(timeColumnIndex).get(start + size - 1);
            if (ts < pre) {
                throw new OutOfOrderException(format("ts: %d, pre: %d, check the ts column value or use greater watermark", ts, pre));
            }
        }

        initTable();

        for (int i = 0; i < this.table.size(); i++) {
            this.table.get(i).add(table.getColumn(i).get(row));
        }

        size++;
    }

    public int countLessThan(long ts) {
        int loc = locate(ts);
        if (-1 == loc) {
            return 0;
        }

        return loc - this.start;
    }

    /**
     * 对于0，1，2，3，5，6，8 执行locate(0)为-1，locate(4)的结果为4，locate(7)的结果为6
     * @param ts
     * @return
     */
    private int locate(long ts) {
        if (size < 1) {
            return -1;
        }

        int left = start;
        int right = start + size;
        int mid;
        long v;
        while (true) {
            if (right == left + 1) {
                v = (long) table.get(timeColumnIndex).get(left);
                if (ts > v) {
                    return right;
                }
                return -1;
            }
            mid = (left + right) / 2;
            v = (long) table.get(timeColumnIndex).get(mid);
            if (ts <= v) {
                right = mid;
            } else {
                left = mid;
            }
        }
    }

    public void removeLessThan(long ts) {
        int loc = locate(ts);
        if (-1 == loc) {
            return;
        }

        size -= loc - start;
        start = loc;
        realRemove();
    }

    public void removeFirstRow() {
        if (size < 1) {
            throw new IllegalStateException("no row");
        }
        start++;
        size--;

        realRemove();
    }

    private void realRemove() {
        if (table.size() < 2) {
            return;
        }

        //删掉的行数超过一半才进行一次重新整理以提升性能，最多浪费一倍内存
        //最坏情况为大量的3条删两条的case这种情况下性能最差但如果增加一个比如超过1000条并且超过一半才删的条件的话可能会造成1000倍的内存浪费以至crash
        if (start > this.table.size() / 2) {
            if (0 == size) {
                this.table = null;
            } else if (size > 0) {
                List<List<Comparable>> table = new ArrayList<>(columnName2Index.size());
                for (int j = 0; j < columnName2Index.size(); j++) {
                    List<Comparable> column = new ArrayList<>(size);
                    for (int i = start; i < start + size; i++) {
                        column.add(this.table.get(j).get(i));
                    }
                    table.add(column);
                }
                this.table = table;
            } else {
                throw new IllegalStateException(format("size: %d", size));
            }
            start = 0;
        }
    }

    public int size() {
        return size;
    }

    public List<Row> rows() {
        List<Row> rows = new ArrayList<>(size);
        for (int i = start; i < start + size; i++) {
            rows.add(new RowByTableHeap(columnName2Index, table, i));
        }

        return rows;
    }

    public long firstTime() {
        return (long) this.table.get(timeColumnIndex).get(start);
    }

    public long lastTime() {
        return (long) this.table.get(timeColumnIndex).get(start + size - 1);
    }
}
