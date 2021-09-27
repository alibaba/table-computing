package com.alibaba.tc.sp.input;

import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.TableBuilder;
import com.alibaba.tc.table.Type;
import com.alibaba.tc.offheap.ByteArray;
import com.alibaba.tc.sp.QueueSizeLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static java.util.Objects.requireNonNull;

public abstract class AbstractStreamTable implements StreamTable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractStreamTable.class);
    protected static final ByteArray __time__ = new ByteArray("__time__");
    protected static final ByteArray __source__ = new ByteArray("__source__");
    protected static final ByteArray __topic__ = new ByteArray("__topic__");
    protected static final ByteArray __category__ = new ByteArray("__category__");
    protected static final ByteArray __machine_uuid__ = new ByteArray("__machine_uuid__");
    protected static final ByteArray __receive_time__ = new ByteArray("__receive_time__");
    protected static final Map<String, ByteArray> reservedColumnNames = new HashMap<String, ByteArray>() {{
        put("__time__", __time__);
        put("__source__", __source__);
        put("__topic__", __topic__);
        put("__category__", __category__);
        put("__machine_uuid__", __machine_uuid__);
        put("__receive_time__", __receive_time__);
    }};

    protected final Map<String, Type> columnTypeMap;
    protected final Table emptyTable;
    protected final Map<String, ByteArray> columnName2ByteArray;
    protected final Set<ByteArray> columnNames;
    protected final List<ByteArray> columns;
    protected final Duration batch = Duration.ofSeconds(1);
    protected final int thread;
    protected final int queueDepth = 100;
    protected final QueueSizeLogger queueSizeLogger = new QueueSizeLogger();
    protected final QueueSizeLogger recordSizeLogger = new QueueSizeLogger();
    protected long sleepMs = 100;

    protected final List<ArrayBlockingQueue<Table>> arrayBlockingQueueList;

    protected AbstractStreamTable(Map<String, Type> columnTypeMap) {
        this(Runtime.getRuntime().availableProcessors(), columnTypeMap);
    }

    protected AbstractStreamTable(int thread, Map<String, Type> columnTypeMap) {
        if (thread < 0) {
            throw new IllegalArgumentException();
        }
        this.thread = thread;

        this.columnTypeMap = requireNonNull(columnTypeMap);
        if (columnTypeMap.size() < 1) {
            throw new IllegalArgumentException();
        }
        this.emptyTable = new TableBuilder(columnTypeMap).build();

        columnName2ByteArray = new HashMap<>();
        columnNames = new HashSet<>();
        columns = new ArrayList<>();
        for (String columnName : columnTypeMap.keySet()) {
            ByteArray name;
            ByteArray reservedColumnName = reservedColumnNames.get(columnName);
            if (null != reservedColumnName) {
                name = reservedColumnName;
            } else {
                name = new ByteArray(columnName);
            }
            columnName2ByteArray.put(columnName, name);
            columnNames.add(name);
            columns.add(name);
        }

        arrayBlockingQueueList = new ArrayList<ArrayBlockingQueue<Table>>(thread) {{
            for (int i = 0; i < thread; i++) {
                add(new ArrayBlockingQueue<>(queueDepth));
            }
        }};
    }

    @Override
    public boolean isFinished() {
        for (int i = 0; i < arrayBlockingQueueList.size(); i++) {
            if (arrayBlockingQueueList.get(i).size() > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    final public Table consume() throws InterruptedException {
        while (true) {
            for (int i = 0; i < arrayBlockingQueueList.size(); i++) {
                Table table = arrayBlockingQueueList.get(i).poll();
                if (null != table) {
                    return table;
                }
            }

            //For no continuous data case return an empty table after sleep 100ms (default)
            // to trigger computing, else the watermark data/window data/rehashed or rebalanced to other server/thread data will never be computed
            Thread.sleep(sleepMs);
            return emptyTable;
        }
    }

    public void sleepMsWhenNoData(long sleepMs) {
        this.sleepMs = sleepMs;
    }
}
