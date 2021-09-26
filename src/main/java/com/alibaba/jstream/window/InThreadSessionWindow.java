package com.alibaba.jstream.window;

import com.alibaba.jstream.function.AggTimeWindowFunction;
import com.alibaba.jstream.function.TimeWindowFunction;
import com.alibaba.jstream.table.Row;
import com.alibaba.jstream.table.SlideTable;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.TableBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.jstream.window.TimeWindow.appendRow;
import static com.alibaba.jstream.window.TimeWindow.appendRows;
import static com.alibaba.jstream.window.TimeWindow.genPartitionKey;
import static com.alibaba.jstream.window.TimeWindow.getPartitionedSlideTable;

class InThreadSessionWindow extends InThreadWindow {
    private static final Logger logger = LoggerFactory.getLogger(InThreadSessionWindow.class);

    private long lastDataTime;
    private long lastDataSystemTime;
    private final Map<List<Comparable>, Long> keyEndTime = new HashMap<>();
    private final TreeMap<Long, Set<List<Comparable>>> sortedByEndTime = new TreeMap<>();
    private final long windowTimeout;
    private final TimeWindowFunction windowFunction;
    private final AggTimeWindowFunction aggTimeWindowFunction;
    private final String[] partitionByColumnNames;
    private final String timeColumnName;

    InThreadSessionWindow(long windowTimeout,
                          TimeWindowFunction windowFunction,
                          AggTimeWindowFunction aggTimeWindowFunction,
                          String[] partitionByColumnNames,
                          String timeColumnName) {
        this.windowTimeout = windowTimeout;
        this.windowFunction = windowFunction;
        this.aggTimeWindowFunction = aggTimeWindowFunction;
        this.partitionByColumnNames = partitionByColumnNames;
        this.timeColumnName = timeColumnName;
    }

    void triggerAllWindowBySchedule(TableBuilder retTable) {
        if (0 == lastDataTime) {
            return;
        }
        long now = System.currentTimeMillis();
        long dataTime = now - lastDataSystemTime + lastDataTime;
        triggerAllWindow(retTable, dataTime);
    }

    private void triggerAllWindow(TableBuilder retTable, long dataTime) {
        List<Long> willRemove = new ArrayList<>();
        for (Long endTime : sortedByEndTime.keySet()) {
            if (dataTime - endTime < windowTimeout) {
                break;
            }

            Set<List<Comparable>> keys = sortedByEndTime.get(endTime);
            for (List<Comparable> key : keys) {
                trigger(retTable, key);
                keyEndTime.remove(key);
                partitionedTables.remove(key);
            }

            willRemove.add(endTime);
        }
        for (Long endTime : willRemove) {
            sortedByEndTime.remove(endTime);
        }
    }

    private void trigger(TableBuilder retTable, List<Comparable> key) {
        SlideTable partitionedTable = partitionedTables.get(key);
        List<Row> rows = partitionedTable.rows();
        if (windowFunction != null) {
            List<Comparable[]> comparablesList = windowFunction.transform(key,
                    rows,
                    partitionedTable.firstTime(),
                    partitionedTable.lastTime() + 1);
            appendRows(retTable, comparablesList);
        } else {
            Comparable[] comparables = aggTimeWindowFunction.agg(key,
                    rows,
                    partitionedTable.firstTime(),
                    partitionedTable.lastTime() + 1);
            appendRow(retTable, comparables);
        }
    }

    void trigger(TableBuilder retTable, Table table, StoreType storeType) {
        long dataTime = (long) table.getColumn(timeColumnName).get(0);
        lastDataTime = dataTime;
        lastDataSystemTime = System.currentTimeMillis();
        triggerAllWindow(retTable, dataTime);
        for (int i = 0; i < table.size(); i++) {
            List<Comparable> key = genPartitionKey(table, i, partitionByColumnNames);
            SlideTable partitionedTable = getPartitionedSlideTable(key, table, partitionedTables, timeColumnName, storeType);

            assert (Long) table.getColumn(timeColumnName).get(i) == dataTime;

            partitionedTable.addRow(table, i);

            Long preEndTime = keyEndTime.get(key);
            Long endTime = partitionedTable.lastTime();
            if (null == preEndTime) {
                sortByEndTime(key, endTime);
                keyEndTime.put(key, endTime);
            } else {
                Set<List<Comparable>> keys = sortedByEndTime.get(preEndTime);
                if (keys.size() <= 1) {
                    sortedByEndTime.remove(preEndTime);
                } else {
                    keys.remove(key);
                }
                sortByEndTime(key, endTime);
                keyEndTime.put(key, endTime);
            }
        }
    }

    private void sortByEndTime(List<Comparable> key, Long endTime) {
        Set<List<Comparable>> keys = sortedByEndTime.get(endTime);
        if (null == keys) {
            keys = new HashSet<>();
            sortedByEndTime.put(endTime, keys);
        }
        keys.add(key);
    }
}
