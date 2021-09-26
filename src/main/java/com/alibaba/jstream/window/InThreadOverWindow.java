package com.alibaba.jstream.window;

import com.alibaba.jstream.function.WindowFunction;
import com.alibaba.jstream.table.Row;
import com.alibaba.jstream.table.SlideTable;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.TableBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.jstream.window.TimeWindow.appendRows;
import static com.alibaba.jstream.window.TimeWindow.genPartitionKey;
import static com.alibaba.jstream.window.TimeWindow.getPartitionedSlideTable;

class InThreadOverWindow extends InThreadWindow {
    private static final Logger logger = LoggerFactory.getLogger(InThreadOverWindow.class);

    private long lastDataTime;
    private long lastDataSystemTime;
    private final long windowSize;
    private final WindowFunction windowFunction;
    private final String[] partitionByColumnNames;
    private final String timeColumnName;

    InThreadOverWindow(long windowSize,
                       WindowFunction windowFunction,
                       String[] partitionByColumnNames,
                       String timeColumnName) {
        this.windowSize = windowSize;
        this.windowFunction = windowFunction;
        this.partitionByColumnNames = partitionByColumnNames;
        this.timeColumnName = timeColumnName;
    }

    void triggerAllWindowBySchedule(TableBuilder retTable, long noDataDelay) {
        long now = System.currentTimeMillis();
        if (0 == lastDataTime) {
            return;
        }
        long dataTime = now - lastDataSystemTime + lastDataTime;
        if (dataTime - noDataDelay >= 0) {
            triggerAllWindow(retTable, start(dataTime));
            return;
        }
    }

    private long start(long dataTime) {
        return dataTime - windowSize + 1;
    }

    private void triggerAllWindow(TableBuilder retTable, long start) {
        List<List<Comparable>> willRemove = new ArrayList<>();
        for (List<Comparable> key : partitionedTables.keySet()) {
            if (trigger(retTable, start, key)) {
                willRemove.add(key);
            }
        }
        for (List<Comparable> key : willRemove) {
            partitionedTables.remove(key);
        }
    }

    private boolean trigger(TableBuilder retTable, long start, List<Comparable> key) {
        SlideTable partitionedTable = partitionedTables.get(key);
        int needCompute = partitionedTable.countLessThan(start);
        if (needCompute > 0) {
            List<Row> rows = partitionedTable.rows();
            List<Comparable[]> comparablesList = windowFunction.transform(key, rows, needCompute);
            appendRows(retTable, comparablesList);
            partitionedTable.removeLessThan(start);

            if (partitionedTable.size() <= 0) {
                return true;
            }
        }

        return false;
    }

    void trigger(TableBuilder retTable, Table table, StoreType storeType) {
        //time column must be bigint else throw exception to remind the user
        long dataTime = (long) table.getColumn(timeColumnName).get(0);
        lastDataTime = dataTime;
        lastDataSystemTime = System.currentTimeMillis();
        long start = dataTime - windowSize + 1;
        for (int i = 0; i < table.size(); i++) {
            List<Comparable> key = genPartitionKey(table, i,partitionByColumnNames);
            SlideTable partitionedTable = getPartitionedSlideTable(key, table, partitionedTables, timeColumnName, storeType);
            if (partitionedTable.size() <= 0) {
                partitionedTable.addRow(table, i);
                continue;
            }

            assert (Long) table.getColumn(timeColumnName).get(i) == dataTime;
            if (trigger(retTable, start, key)) {
                partitionedTables.remove(key);
            }

            partitionedTable.addRow(table, i);
        }
    }
}
