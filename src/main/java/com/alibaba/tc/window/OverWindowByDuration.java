package com.alibaba.tc.window;

import com.alibaba.tc.function.WindowFunction;
import com.alibaba.tc.table.Row;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.TableBuilder;
import org.apache.http.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.tc.window.StoreType.STORE_BY_COLUMN;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class OverWindowByDuration extends TimeWindow {
    private static final Logger logger = LoggerFactory.getLogger(OverWindowByDuration.class);

    private final long windowSizeDurationMs;
    private final String[] partitionByColumnNames;
    private final WindowFunction windowFunction;
    private final String[] returnedColumnNames;
    private final Map<Thread, InThreadOverWindow> threadWindow = new ConcurrentHashMap<>();

    public OverWindowByDuration(Duration windowSizeDuration,
                                String[] partitionByColumnNames,
                                String timeColumnName,
                                WindowFunction windowFunction,
                                String... addedColumnNames) {
        this(windowSizeDuration, partitionByColumnNames, timeColumnName, windowFunction, STORE_BY_COLUMN, addedColumnNames);
    }

    public OverWindowByDuration(Duration windowSizeDuration,
                                String[] partitionByColumnNames,
                                String timeColumnName,
                                WindowFunction windowFunction,
                                StoreType storeType,
                                String... returnedColumnNames) {
        super(storeType, timeColumnName);
        this.windowSizeDurationMs = requireNonNull(windowSizeDuration).toMillis();
        this.partitionByColumnNames = requireNonNull(partitionByColumnNames);
        if (partitionByColumnNames.length < 1) {
            throw new IllegalArgumentException("at least one partition by column");
        }

        this.windowFunction = requireNonNull(windowFunction);
        this.returnedColumnNames = requireNonNull(returnedColumnNames);
        if (returnedColumnNames.length < 1) {
            throw new IllegalArgumentException("at least one returned column");
        }
    }

    /**
     *
     * @param tables should be returned by Rehash.rehash() else may occur to logical error. you can only rehash once then use in multiple windows. we
     *               auto rehash in window operation may lead to low performance.
     * @return
     */
    public Table over(List<Table> tables) {
        checkTablesSize(tables);

        Thread curThread = Thread.currentThread();
        InThreadOverWindow inThreadWindow = threadWindow.get(curThread);
        if (null == inThreadWindow) {
            inThreadWindow = new InThreadOverWindow(windowSizeDurationMs,
                    windowFunction,
                    partitionByColumnNames,
                    timeColumnName);
            threadWindow.put(curThread, inThreadWindow);
        }

        tables = watermark(tables);

        TableBuilder retTable = newTableBuilder(returnedColumnNames);
        boolean noData = true;
        for (Table table : tables) {
            if (table.size() > 0) {
                noData = false;
                inThreadWindow.trigger(retTable, table, storeType);
            }
        }
        if (noData) {
            inThreadWindow.triggerAllWindowBySchedule(retTable, noDataDelay);
            return retTable.build();
        }

        return retTable.build();
    }

    @Override
    public List<Row> getRows(List<Comparable> partitionBy) {
        InThreadOverWindow inThreadOverWindow = threadWindow.get(Thread.currentThread());
        return inThreadOverWindow.getRows(partitionBy);
    }
}
