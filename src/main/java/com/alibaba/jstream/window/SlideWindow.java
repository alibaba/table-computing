package com.alibaba.jstream.window;

import com.alibaba.jstream.exception.OutOfOrderException;
import com.alibaba.jstream.function.AggTimeWindowFunction;
import com.alibaba.jstream.function.TimeWindowFunction;
import com.alibaba.jstream.table.Column;
import com.alibaba.jstream.table.Row;
import com.alibaba.jstream.table.RowByTable;
import com.alibaba.jstream.table.SlideTable;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.TableBuilder;
import org.apache.http.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.Threads.threadsNamed;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SlideWindow extends TimeWindow {
    private static final Logger logger = LoggerFactory.getLogger(SlideWindow.class);

    private static class WindowTime {
        long startTime;
        long lastDataTime;
        long lastDataSystemTime;
    }

    private static List<SlideWindow> slideWindows = new ArrayList<>();

    static {
        new ScheduledThreadPoolExecutor(1, threadsNamed("HopWindowLogger")).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (SlideWindow slideWindow : slideWindows) {
                    synchronized (slideWindow) {
                        if (slideWindow.timesExceed > 0) {
                            logger.warn("{}: dataTime exceed window start, times: {}, maxGap: {}, " +
                                            "use watermark to avoid(windowSize too small or " +
                                            "noDataDelay too small also lead to this case). {}",
                                    slideWindow.sign,
                                    slideWindow.timesExceed,
                                    slideWindow.maxGapExceed,
                                    slideWindow.outOfOrderException);
                            slideWindow.timesExceed = 0;
                        }
                        if (slideWindow.timesBehind > 0) {
                            logger.warn("{}: dataTime behind window start, times: {}, maxGap: {}, " +
                                            "use watermark to avoid(windowSize too small or " +
                                            "noDataDelay too small also lead to this case). {}",
                                    slideWindow.sign,
                                    slideWindow.timesBehind,
                                    slideWindow.maxGapBehind,
                                    slideWindow.outOfOrderException);
                            slideWindow.timesBehind = 0;
                        }
                        slideWindow.outOfOrderException = null;
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private final long windowSizeDurationMs;
    private final long slideDurationMs;
    private final String[] partitionByColumnNames;
    private final TimeWindowFunction windowFunction;
    private final AggTimeWindowFunction aggTimeWindowFunction;
    private final String[] columnNames;
    private final Map<Thread, Map<List<Comparable>, SlideTable>> threadPartitionedTables = new ConcurrentHashMap<>();
    private final Map<Thread, WindowTime> windowTimeMap = new ConcurrentHashMap<>();
    private final String sign;

    private int timesExceed;
    private int timesBehind;
    private long maxGapExceed = 0;
    private long maxGapBehind = 0;
    private OutOfOrderException outOfOrderException;

    private synchronized void warn(long dataTime, long windowStartTime, OutOfOrderException outOfOrderException) {
        if (dataTime < windowStartTime) {
            if (dataTime - windowStartTime < maxGapBehind) {
                maxGapBehind = dataTime - windowStartTime;
            }
            timesBehind++;
        } else {
            if (dataTime - windowStartTime - windowSizeDurationMs > maxGapExceed) {
                maxGapExceed = dataTime - windowStartTime - windowSizeDurationMs;
            }
            timesExceed++;
        }
        this.outOfOrderException = outOfOrderException;
    }

    public SlideWindow(Duration windowSizeDuration,
                       Duration slideDuration,
                       String[] partitionByColumnNames,
                       String timeColumnName,
                       AggTimeWindowFunction aggTimeWindowFunction,
                       String... returnedColumnNames) {
        this(windowSizeDuration,
                slideDuration,
                partitionByColumnNames,
                timeColumnName,
                null,
                aggTimeWindowFunction,
                StoreType.STORE_BY_COLUMN,
                returnedColumnNames);
    }

    public SlideWindow(Duration windowSizeDuration,
                       Duration slideDuration,
                       String[] partitionByColumnNames,
                       String timeColumnName,
                       TimeWindowFunction windowFunction,
                       String... returnedColumnNames) {
        this(windowSizeDuration,
                slideDuration,
                partitionByColumnNames,
                timeColumnName,
                windowFunction,
                null,
                StoreType.STORE_BY_COLUMN,
                returnedColumnNames);
    }

    public SlideWindow(Duration windowSizeDuration,
                       Duration slideDuration,
                       String[] partitionByColumnNames,
                       String timeColumnName,
                       AggTimeWindowFunction aggTimeWindowFunction,
                       StoreType storeType,
                       String... returnedColumnNames) {
        this(windowSizeDuration,
                slideDuration,
                partitionByColumnNames,
                timeColumnName,
                null,
                aggTimeWindowFunction,
                storeType,
                returnedColumnNames);
    }

    public SlideWindow(Duration windowSizeDuration,
                       Duration slideDuration,
                       String[] partitionByColumnNames,
                       String timeColumnName,
                       TimeWindowFunction windowFunction,
                       StoreType storeType,
                       String... returnedColumnNames) {
        this(windowSizeDuration,
                slideDuration,
                partitionByColumnNames,
                timeColumnName,
                windowFunction,
                null,
                storeType,
                returnedColumnNames);
    }

    private SlideWindow(Duration windowSizeDuration,
                        Duration slideDuration,
                        String[] partitionByColumnNames,
                        String timeColumnName,
                        TimeWindowFunction windowFunction,
                        AggTimeWindowFunction aggTimeWindowFunction,
                        StoreType storeType,
                        String... returnedColumnNames) {
        super(storeType, timeColumnName);
        this.windowSizeDurationMs = requireNonNull(windowSizeDuration).toMillis();
        this.slideDurationMs = requireNonNull(slideDuration).toMillis();
        if (windowSizeDurationMs <= 0) {
            throw new IllegalArgumentException("windowSizeDuration should be greater than 0ms");
        }
        if (slideDurationMs <= 0) {
            throw new IllegalArgumentException("hopDuration should be greater than 0ms");
        }
        if (slideDurationMs > windowSizeDurationMs) {
            throw new IllegalArgumentException("hopDuration should be less or equal to windowSizeDuration");
        }
        this.partitionByColumnNames = requireNonNull(partitionByColumnNames);
        if (partitionByColumnNames.length < 1) {
            throw new IllegalArgumentException("at least one partition by column");
        }
        this.windowFunction = windowFunction;
        this.aggTimeWindowFunction = aggTimeWindowFunction;
        this.columnNames = requireNonNull(returnedColumnNames);
        if (columnNames.length < 1) {
            throw new IllegalArgumentException("at least one returned column");
        }
        this.sign = "HopWindow|" + join(",", partitionByColumnNames) + "|" + timeColumnName + "|" + windowSizeDuration + "|" + slideDuration;

        slideWindows.add(this);
    }

    private void enterWindow(Table table,
                             int row,
                             Map<List<Comparable>, SlideTable> partitionedTables) {
        getPartitionedSlideTable(genPartitionKey(table, row, partitionByColumnNames),
                table,
                partitionedTables,
                timeColumnName,
                storeType)
                .addRow(table, row);
    }

    private void triggerAllWindow(TableBuilder retTable,
                                  WindowTime windowTime,
                                  Map<List<Comparable>, SlideTable> partitionedTables) {
        List<List<Comparable>> willRemove = new ArrayList<>();
        for (List<Comparable> key : partitionedTables.keySet()) {
            SlideTable partitionedTable = partitionedTables.get(key);
            List<Row> rows = partitionedTable.rows();
            appendRow(retTable, key, rows, windowTime.startTime, windowTime.startTime + windowSizeDurationMs);
            partitionedTable.removeLessThan(windowTime.startTime + slideDurationMs);
            int size = partitionedTable.size();
            if (0 == size) {
                willRemove.add(key);
            } else if (size < 0) {
                throw new IllegalStateException(format("partitionedTable.size()：%d", partitionedTable.size()));
            }
        }
        for (List<Comparable> key : willRemove) {
            partitionedTables.remove(key);
        }
    }

    private void appendRow(TableBuilder retTable, List<Comparable> key, List<Row> rows, long windowStart, long windowEnd) {
        if (windowFunction != null) {
            List<Comparable[]> comparablesList = windowFunction.transform(key,
                    rows,
                    windowStart,
                    windowEnd);
            appendRows(retTable, comparablesList);
        } else {
            Comparable[] comparables = aggTimeWindowFunction.agg(key,
                    rows,
                    windowStart,
                    windowEnd);
            appendRow(retTable, comparables);
        }
    }

    private void triggerOneElemWindow(TableBuilder retTable, Table table, int i, long elemDataTime) {
        List<Comparable> key = genPartitionKey(table, i, partitionByColumnNames);
        long windowStart = elemDataTime / windowSizeDurationMs * windowSizeDurationMs;
        Row row = new RowByTable(table, i);
        List<Row> rows = new ArrayList<>(1);
        rows.add(row);
        appendRow(retTable, key, rows, windowStart, windowStart + windowSizeDurationMs);
    }

    public Table slide(List<Table> tables) {
        checkTablesSize(tables);
        TableBuilder retTable = newTableBuilder(columnNames);
        tables = watermark(tables);

        Thread curThread = Thread.currentThread();
        Map<List<Comparable>, SlideTable> partitionedTables = threadPartitionedTables.get(curThread);
        if (null == partitionedTables) {
            partitionedTables = new HashMap<>();
            threadPartitionedTables.put(curThread, partitionedTables);
        }

        WindowTime windowTime = windowTimeMap.get(curThread);

        boolean noData = true;
        for (Table table : tables) {
            if (table.size() > 0) {
                noData = false;
                windowTime = hopOneTable(retTable, table, windowTime, curThread, partitionedTables);
            }
        }
        if (noData) {
            if (windowTime == null) {
                return retTable.build();
            }
            long now = System.currentTimeMillis();
            if (now - windowTime.lastDataSystemTime > noDataDelay) {
                long dataTime = now - windowTime.lastDataSystemTime + windowTime.lastDataTime;
                if (dataTime >= windowTime.startTime + windowSizeDurationMs) {
                    triggerAllWindow(retTable, windowTime, partitionedTables);
                    windowTime.startTime = dataTime / windowSizeDurationMs * windowSizeDurationMs;
                    logger.info("no data window advanced, now: {}, lastDataSystemTime: {}, " +
                                    "partitionByColumnNames: {}, timeColumnName: {}",
                            now,
                            windowTime.lastDataSystemTime,
                            partitionByColumnNames,
                            timeColumnName);
                    return retTable.build();
                }
            }
            return retTable.build();
        }

        return retTable.build();
    }

    private WindowTime hopOneTable(TableBuilder retTable,
                                   Table table,
                                   WindowTime windowTime,
                                   Thread curThread,
                                   Map<List<Comparable>, SlideTable> partitionedTables) {
        //时间列必须是bigint Long类型否则抛异常让用户感知到
        long dataTime = (long) table.getColumn(timeColumnName).get(0);
        if (null == windowTime) {
            windowTime = new WindowTime();
            windowTimeMap.put(curThread, windowTime);
            windowTime.startTime = dataTime / windowSizeDurationMs * windowSizeDurationMs;
        }
        long now = System.currentTimeMillis();
        windowTime.lastDataTime = dataTime;
        windowTime.lastDataSystemTime = now;

        for (int i = 0; i < table.size(); i++) {
            try {
                //时间列必须是bigint Long类型否则抛异常让用户感知到
                dataTime = (long) table.getColumn(timeColumnName).get(i);
                if (dataTime >= windowTime.startTime + windowSizeDurationMs) {
                    triggerAllWindow(retTable, windowTime, partitionedTables);
                    windowTime.startTime += slideDurationMs;

                    if (dataTime >= windowTime.startTime + windowSizeDurationMs) {
                        triggerOneElemWindow(retTable, table, i, dataTime);
                        warn(dataTime, windowTime.startTime, null);
                    } else {
                        enterWindow(table, i, partitionedTables);
                    }
                } else {
                    if (dataTime < windowTime.startTime) {
                        triggerOneElemWindow(retTable, table, i, dataTime);
                        warn(dataTime, windowTime.startTime, null);
                    } else {
                        enterWindow(table, i, partitionedTables);
                    }
                }
            } catch (OutOfOrderException e) {
                warn(dataTime, windowTime.startTime, e);
            }
        }

        return windowTime;
    }

    @Override
    public List<Row> getRows(List<Comparable> partitionBy) {
        SlideTable partitionedTable = threadPartitionedTables.get(Thread.currentThread()).get(partitionBy);
        if (null == partitionedTable) {
            return null;
        }
        return partitionedTable.rows();
    }
}
