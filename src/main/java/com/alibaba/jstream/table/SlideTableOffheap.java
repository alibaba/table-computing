package com.alibaba.jstream.table;

import com.alibaba.jstream.exception.OutOfOrderException;
import org.apache.arrow.util.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.jstream.table.Table.createEmptyTableLike;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SlideTableOffheap implements SlideTable {
    private Table table;
    private final String timeColumnName;
    private final int timeColumnIndex;
    private int start;
    private int size;

    public SlideTableOffheap(Table table, String timeColumnName) {
        this.timeColumnName = requireNonNull(timeColumnName);
        this.timeColumnIndex = table.getIndex(timeColumnName);
    }

    @Override
    public void addRow(Table table, int row) {
        long ts = (long) table.getColumn(timeColumnIndex).get(row);
        if (size > 0) {
            long pre = (long) this.table.getColumn(timeColumnIndex).get(start + size - 1);
            if (ts < pre) {
                throw new OutOfOrderException(format("ts: %d, pre: %d", ts, pre));
            }
        }

        if (null == this.table) {
            //懒加载。需要的时候才创建表
            this.table = createEmptyTableLike(table);
        }
        this.table.append(table, row);

        size++;
    }

    @Override
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
        Column column = table.getColumn(timeColumnIndex);
        int left = start;
        int right = start + size;
        int mid;
        long v;
        while (true) {
            if (right == left + 1) {
                v = (long) column.get(left);
                if (ts > v) {
                    return right;
                }
                return -1;
            }
            mid = (left + right) / 2;
            v = (long) column.get(mid);
            if (ts <= v) {
                right = mid;
            } else {
                left = mid;
            }
        }
    }

    @Override
    public void removeLessThan(long ts) {
        int loc = locate(ts);
        if (-1 == loc) {
            return;
        }

        size -= loc - start;
        start = loc;
        realRemove();
    }

    @Override
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
                //懒加载。后面需要的时候再创建表，size==0之后SortedTableByTime被整个释放掉的概率很大
                this.table = null;
            } else if (size > 0) {
                Table table = createEmptyTableLike(this.table);
                for (int i = start; i < start + size; i++) {
                    table.append(this.table, i);
                }
                this.table = table;
            } else {
                throw new IllegalStateException(format("size: %d", size));
            }
            start = 0;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @VisibleForTesting
    public int tableSize() {
        return table.size();
    }

    @Override
    public List<Row> rows() {
        List<Row> rows = new ArrayList<>(size);
        for (int i = start; i < start + size; i++) {
            rows.add(new RowByTable(table, i));
        }

        return rows;
    }

    @Override
    public long firstTime() {
        return (long) table.getColumn(timeColumnIndex).get(start);
    }

    @Override
    public long lastTime() {
        return (long) table.getColumn(timeColumnIndex).get(start + size - 1);
    }
}
