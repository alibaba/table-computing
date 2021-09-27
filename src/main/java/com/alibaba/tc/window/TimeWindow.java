package com.alibaba.tc.window;

import com.alibaba.tc.table.Column;
import com.alibaba.tc.table.Row;
import com.alibaba.tc.table.SlideTable;
import com.alibaba.tc.table.SlideTableByColumn;
import com.alibaba.tc.table.SlideTableByRow;
import com.alibaba.tc.table.SlideTableOffheap;
import com.alibaba.tc.table.SortedTable;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.TableBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.tc.table.Table.createEmptyTableLike;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class TimeWindow extends Window {
    /**
     * 无数据后多长时间（持续多长时间没有数据）由系统决定窗口触发和前进
     * 系统触发逻辑：通过如下公式得到当前应该的数据时间通过这一时间来触发（注意该公式不存在误差累积的问题）
     * 数据时间 = 当前系统时间 - 最后一条数据的系统时间 + 最后一条数据的数据时间
     * 注意：该值过小会使系统提前触发窗口计算使窗口前进导致数据滞后窗口，该值过大会使没有数据的情况下窗口触发不及时
     */
    protected long noDataDelay = 5000;
    protected long watermark = 1000;
    protected final String timeColumnName;
    protected final StoreType storeType;

    @Override
    public List<Row> getRows(List<Comparable> partitionBy) {
        return null;
    }

    private static class InThread {
        long lastDataTime;
        long lastDataSystemTime;
        TreeMap<Long, Table> tables;
    }

    private Map<Thread, InThread> threads = new ConcurrentHashMap<>();

    public TimeWindow(String timeColumnName) {
        this(StoreType.STORE_BY_COLUMN, timeColumnName);
    }

    protected TimeWindow(StoreType storeType, String timeColumnName) {
        this.storeType = requireNonNull(storeType);
        this.timeColumnName = requireNonNull(timeColumnName);
    }

    /**
     * 超过窗口大小后持续多长时间没有数据的情况下触发窗口计算
     * 默认值： 100ms 该值太小会导致数据只是差很短的时间没有及时到达就被认为窗口该由于数据不连续没法触发而应该由系统时间触发了
     *
     * @param noDataDelay
     */
    public void setNoDataDelay(Duration noDataDelay) {
        this.noDataDelay = noDataDelay.toMillis();
    }

    public void setWatermark(Duration watermark) {
        this.watermark = watermark.toMillis();
    }

    private List<Table> matureTables(long dataTime, TreeMap<Long, Table> tables) {
        List<Table> retTables = new ArrayList<>();
        List<Long> willRemove = new ArrayList<>();
        for (Long time : tables.keySet()) {
            if (dataTime - time < watermark) {
                break;
            }
            willRemove.add(time);
        }
        for (Long time : willRemove) {
            retTables.add(tables.remove(time));
        }

        return retTables;
    }

    public List<Table> watermark(Table table) {
        return watermark(new ArrayList<Table>(1) {{
            add(table);
        }});
    }

    public List<Table> watermark(List<Table> tables) {
        Thread curThread = Thread.currentThread();
        InThread inThread = threads.get(curThread);
        if (null == inThread) {
            inThread = new InThread();
            inThread.tables = new TreeMap<>();
            threads.put(curThread, inThread);
        }

        long maxDataTime = 0;
        for (Table table : tables) {
            maxDataTime = max(maxDataTime, watermark(table, inThread));
        }

        long now = System.currentTimeMillis();
        if (0 == maxDataTime) {
            return matureTables(now - inThread.lastDataSystemTime + inThread.lastDataTime, inThread.tables);
        } else if (maxDataTime < 0) {
            throw new IllegalStateException();
        }

        inThread.lastDataTime = maxDataTime;
        inThread.lastDataSystemTime = now;

        return matureTables(maxDataTime, inThread.tables);
    }

    private long watermark(Table table, InThread inThread) {
        long maxDataTime = 0;
        for (int i = 0; i < table.size(); i++) {
            long dataTime = (long) table.getColumn(timeColumnName).get(i);
            if (dataTime > maxDataTime) {
                maxDataTime = dataTime;
            }
            Table tmp = inThread.tables.get(dataTime);
            if (null == tmp) {
                tmp = createEmptyTableLike(table);
                inThread.tables.put(dataTime, tmp);
            }
            tmp.append(table, i);
        }
        return maxDataTime;
    }

    static void appendRow(TableBuilder retTable, Comparable[] comparables) {
        if (null == comparables) {
            return;
        }
        for (int i = 0; i < retTable.columnSize(); i++) {
            retTable.appendValue(i, comparables[i]);
        }
    }

    static TableBuilder newTableBuilder(String[] columnNames) {
        List<Column> columns = new ArrayList<>(columnNames.length);

        for (int i = 0; i < columnNames.length; i++) {
            columns.add(new Column(columnNames[i]));
        }

        return new TableBuilder(columns);
    }

    static void appendRows(TableBuilder retTable, List<Comparable[]> comparablesList) {
        if (null == comparablesList) {
            return;
        }
        for (Comparable[] comparables : comparablesList) {
            appendRow(retTable, comparables);
        }
    }

    static void appendRows(TableBuilder retTable, List<Row> rows, List<Comparable[]> comparablesList, int size) {
        if (null == comparablesList) {
            return;
        }
        for (int i = 0; i < size; i++) {
            int j = 0;
            int preColumnCount = rows.get(i).size();
            for (; j < preColumnCount; j++) {
                retTable.appendValue(j, rows.get(i).get(j));
            }
            for (; j < retTable.columnSize(); j++) {
                if (null == comparablesList ||
                        i >= comparablesList.size() ||
                        null == comparablesList.get(i) ||
                        j - preColumnCount >= comparablesList.get(i).length) {
                    retTable.appendValue(j, null);
                } else {
                    retTable.appendValue(j, comparablesList.get(i)[j - preColumnCount]);
                }
            }
        }
    }

    static List<Comparable> genPartitionKey(Table table, int row, String[] partitionByColumnNames) {
        List<Comparable> key = new ArrayList<>(partitionByColumnNames.length);
        for (int j = 0; j < partitionByColumnNames.length; j++) {
            key.add(table.getColumn(partitionByColumnNames[j]).get(row));
        }

        return key;
    }

    static SortedTable getPartitionedTable(List<Comparable> key,
                                           Table table,
                                           Map<List<Comparable>, SortedTable> partitionedTables,
                                           String... timeColumnName) {
        SortedTable partitionedTable = partitionedTables.get(key);
        if (null == partitionedTable) {
            partitionedTable = new SortedTable(table, timeColumnName);
            partitionedTables.put(key, partitionedTable);
        }

        return partitionedTable;
    }

    static SlideTable getPartitionedSlideTable(List<Comparable> key,
                                                  Table table,
                                                  Map<List<Comparable>, SlideTable> partitionedTables,
                                                  String timeColumnName,
                                                  StoreType storeType) {
        SlideTable partitionedTable = partitionedTables.get(key);
        if (null == partitionedTable) {
            switch (storeType) {
                case STORE_BY_ROW:
                    partitionedTable = new SlideTableByRow(table, timeColumnName);
                    break;
                case STORE_ON_OFFHEAP:
                    partitionedTable = new SlideTableOffheap(table, timeColumnName);
                    break;
                default:
                    partitionedTable = new SlideTableByColumn(table, timeColumnName);
                    break;
            }
            partitionedTables.put(key, partitionedTable);
        }

        return partitionedTable;
    }
}
