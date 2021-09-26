package com.alibaba.jstream.sp.input;

import com.alibaba.jstream.table.TableBuilder;
import com.alibaba.jstream.table.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class InsertableStreamTable extends AbstractStreamTable {
    private static final Logger logger = LoggerFactory.getLogger(InsertableStreamTable.class);

    public InsertableStreamTable(Map<String, Type> columnTypeMap) {
        super(columnTypeMap);
    }

    public InsertableStreamTable(int thread, Map<String, Type> columnTypeMap) {
        super(thread, columnTypeMap);
    }

    public void insert(int threadId, Comparable... values) {
        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
        for (int i = 0; i < values.length; i++) {
            tableBuilder.appendValue(i, values[i]);
        }
        try {
            arrayBlockingQueueList.get(threadId).put(tableBuilder.build());
        } catch (InterruptedException e) {
            logger.error("", e);
        }
    }

    @Override
    public boolean isFinished() {
        return super.isFinished();
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
