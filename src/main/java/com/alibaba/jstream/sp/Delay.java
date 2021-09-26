package com.alibaba.jstream.sp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.Threads.threadsNamed;

public class Delay {
    private static final Logger logger = LoggerFactory.getLogger(Delay.class);

    public static final Delay DELAY = new Delay("delay", true);
    public static final Delay RESIDENCE_TIME = new Delay("residence-time", false);
    private static final long PERIOD = 1;

    private final Map<String, Long> stats = new ConcurrentHashMap();
    private final List<String> names = new ArrayList<>();
    private volatile long preLogTime = 0;

    private Delay(String threadName, boolean needSub) {
        new ScheduledThreadPoolExecutor(1, threadsNamed(threadName)).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                synchronized (names) {
                    for (String name : names) {
                        long ms = stats.get(name);
                        if (needSub) {
                            ms = System.currentTimeMillis() - ms;
                        }
                        long second = ms/1000;
                        long hour = second/3600;
                        long minute = second%3600/60;
                        second %= 60;
                        ms %= 1000;
                        StringBuilder sb = new StringBuilder(24);
                        if (0 != hour) {
                            sb.append(hour).append("小时");
                        }
                        if (0 != minute) {
                            sb.append(minute).append("分钟");
                        }
                        if (0 != second) {
                            sb.append(second).append("秒");
                        }
                        sb.append(ms).append("毫秒");
                        logger.info("{}: {}", name, sb.toString());
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void log(String name, long ms) {
        long now = System.currentTimeMillis();
        if (now - preLogTime < PERIOD) {
            return;
        }
        preLogTime = now;
        if (!names.contains(name)) {
            synchronized (names) {
                if (!names.contains(name)) {
                    names.add(name);
                }
            }
        }
        stats.put(name, ms);
    }
}
