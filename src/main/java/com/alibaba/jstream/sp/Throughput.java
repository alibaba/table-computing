package com.alibaba.jstream.sp;

import com.alibaba.jstream.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.jstream.Threads.threadsNamed;

public class Throughput {
    private static final Logger logger = LoggerFactory.getLogger(Throughput.class);

    private static final Map<String, AtomicLong> stats = new ConcurrentHashMap();
    private static final List<String> names = new ArrayList<>();

    static {
        new ScheduledThreadPoolExecutor(1, threadsNamed("Throughput")).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (String name : names) {
                    logger.info("{}: {}/s", name, stats.get(name).getAndSet(0)/5);
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public static void log(String name, List<Table> tables) {
        for (Table table : tables) {
            log(name, table);
        }
    }

    public static void log(String name, Table table) {
        if (!names.contains(name)) {
            synchronized (names) {
                if (!names.contains(name)) {
                    names.add(name);
                }
            }
        }
        stats.putIfAbsent(name, new AtomicLong());
        stats.get(name).addAndGet(table.size());
    }
}
