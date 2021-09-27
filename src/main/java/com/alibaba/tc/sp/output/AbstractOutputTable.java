package com.alibaba.tc.sp.output;

import com.alibaba.tc.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class AbstractOutputTable implements OutputTable {
    protected static final String __time__ = "__time__";

    protected final List<ArrayBlockingQueue<Table>> arrayBlockingQueueList;
    protected final int thread;
    protected final int queueDepth = 100;
    private final Random random = new Random();

    protected AbstractOutputTable(int thread) {
        this.thread = thread;
        arrayBlockingQueueList = new ArrayList<>(thread);
        for (int i = 0; i < thread; i++) {
            arrayBlockingQueueList.add(new ArrayBlockingQueue<>(queueDepth));
        }
    }

    protected void putTable(Table table) throws InterruptedException {
        arrayBlockingQueueList.get(random()).put(table);
    }

    final protected Table consume() throws InterruptedException {
        while (true) {
            for (ArrayBlockingQueue<Table> arrayBlockingQueue : arrayBlockingQueueList) {
                Table table = arrayBlockingQueue.poll();
                if (null != table) {
                    return table;
                }
            }

            Thread.sleep(100);
        }
    }

    private int random() {
        return random.nextInt(thread);
    }
}
