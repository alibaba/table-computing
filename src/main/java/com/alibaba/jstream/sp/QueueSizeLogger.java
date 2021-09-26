package com.alibaba.jstream.sp;

import com.alibaba.jstream.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.Threads.threadsNamed;

public class QueueSizeLogger {
    private static final Logger logger = LoggerFactory.getLogger(QueueSizeLogger.class);
    private static final long PERIOD = 1000;

    private static final Map<String, Long> stats = new ConcurrentHashMap();
    private static final List<String> names = new ArrayList<>();
    private volatile long preLogTime = 0;

    static {
        new ScheduledThreadPoolExecutor(1, threadsNamed("QueueSizeLogger")).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (String name : names) {
                    logger.info("{}: {}", name, stats.get(name));
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void logQueueSize(String name, Collection collections) {
        long now = System.currentTimeMillis();
        if (now - preLogTime < PERIOD) {
            return;
        }
        preLogTime = now;

        long sum = 0;
        for (Object collection : collections) {
            sum += ((Collection) collection).size();
        }
        logSize(name, sum);
    }

    public void logRecordSize(String name, Collection collections) {
        long now = System.currentTimeMillis();
        if (now - preLogTime < PERIOD) {
            return;
        }
        preLogTime = now;

        long sum = 0;
        for (Object collection : collections) {
            ArrayBlockingQueue<Table> queue = (ArrayBlockingQueue<Table>) collection;
            for (Table table : queue) {
                sum += table.size();
            }
        }
        logSize(name, sum);
    }

    private void logSize(String name, long size) {
        if (!names.contains(name)) {
            synchronized (names) {
                if (!names.contains(name)) {
                    names.add(name);
                }
            }
        }
        stats.put(name, size);
    }
}
