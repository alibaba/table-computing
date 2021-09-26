package com.alibaba.jstream.window;

import com.alibaba.jstream.function.AggTimeWindowFunction;
import com.alibaba.jstream.function.TimeWindowFunction;
import com.alibaba.jstream.table.Column;
import com.alibaba.jstream.table.Row;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.TableBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.jstream.window.StoreType.STORE_BY_COLUMN;
import static java.util.Objects.requireNonNull;

public class SessionWindow extends TimeWindow {
    private static final Logger logger = LoggerFactory.getLogger(SessionWindow.class);

    private final long windowTimeoutMs;
    private final String[] partitionByColumnNames;
    private final TimeWindowFunction windowFunction;
    private final AggTimeWindowFunction aggTimeWindowFunction;
    private final String[] returnedColumnNames;
    private final Map<Thread, InThreadSessionWindow> threadWindow = new ConcurrentHashMap<>();

    public SessionWindow(Duration windowTimeout,
                         String[] partitionByColumnNames,
                         String timeColumnName,
                         TimeWindowFunction windowFunction,
                         String... returnedColumnNames) {
        this(windowTimeout, partitionByColumnNames, timeColumnName, windowFunction, null, STORE_BY_COLUMN, returnedColumnNames);
    }

    public SessionWindow(Duration windowTimeout,
                         String[] partitionByColumnNames,
                         String timeColumnName,
                         TimeWindowFunction windowFunction,
                         StoreType storeType,
                         String... returnedColumnNames) {
        this(windowTimeout, partitionByColumnNames, timeColumnName, windowFunction, null, storeType, returnedColumnNames);
    }

    public SessionWindow(Duration windowTimeout,
                     String[] partitionByColumnNames,
                     String timeColumnName,
                     AggTimeWindowFunction aggTimeWindowFunction,
                     String... returnedColumnNames) {
        this(windowTimeout, partitionByColumnNames, timeColumnName, null, aggTimeWindowFunction, STORE_BY_COLUMN, returnedColumnNames);
    }

    public SessionWindow(Duration windowTimeout,
                         String[] partitionByColumnNames,
                         String timeColumnName,
                         AggTimeWindowFunction aggTimeWindowFunction,
                         StoreType storeType,
                         String... returnedColumnNames) {
        this(windowTimeout, partitionByColumnNames, timeColumnName, null, aggTimeWindowFunction, storeType, returnedColumnNames);
    }

    private SessionWindow(Duration windowTimeout,
                        String[] partitionByColumnNames,
                        String timeColumnName,
                        TimeWindowFunction windowFunction,
                        AggTimeWindowFunction aggTimeWindowFunction,
                        StoreType storeType,
                        String... returnedColumnNames) {
        super(storeType, timeColumnName);
        this.windowTimeoutMs = requireNonNull(windowTimeout).toMillis();
        if (windowTimeoutMs <= 0) {
            throw new IllegalArgumentException("windowTimeout should be greater than 0ms");
        }
        this.partitionByColumnNames = requireNonNull(partitionByColumnNames);
        if (partitionByColumnNames.length < 1) {
            throw new IllegalArgumentException("at least one partition by column");
        }

        this.windowFunction = windowFunction;
        this.aggTimeWindowFunction = aggTimeWindowFunction;
        this.returnedColumnNames = requireNonNull(returnedColumnNames);
        if (returnedColumnNames.length < 1) {
            throw new IllegalArgumentException("at least one returned column");
        }
    }

    public Table session(List<Table> tables) {
        checkTablesSize(tables);
        Thread curThread = Thread.currentThread();
        InThreadSessionWindow inThreadWindow = threadWindow.get(curThread);
        if (null == inThreadWindow) {
            inThreadWindow = new InThreadSessionWindow(windowTimeoutMs,
                    windowFunction,
                    aggTimeWindowFunction,
                    partitionByColumnNames,
                    timeColumnName);
            threadWindow.put(curThread, inThreadWindow);
        }

        TableBuilder retTable = newTableBuilder(returnedColumnNames);

        tables = watermark(tables);

        boolean noData = true;
        for (Table table1 : tables) {
            if (table1.size() > 0) {
                noData = false;
                inThreadWindow.trigger(retTable, table1, storeType);
            }
        }
        if (noData) {
            inThreadWindow.triggerAllWindowBySchedule(retTable);
            return retTable.build();
        }

        return retTable.build();
    }

    @Override
    public List<Row> getRows(List<Comparable> partitionBy) {
        InThreadSessionWindow inThreadSessionWindow = threadWindow.get(Thread.currentThread());
        return inThreadSessionWindow.getRows(partitionBy);
    }
}
