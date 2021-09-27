package com.alibaba.tc.sp.input;

import com.alibaba.tc.SystemProperty;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.Type;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.tc.sp.StreamProcessing.handleException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MysqlStreamTable extends AbstractStreamTable {
    private static final Logger logger = LoggerFactory.getLogger(MysqlStreamTable.class);
    private final String url;
    private final String userName;
    private final String password;
    private final String sql;
    private final String myName;
    private final int batchSize;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final AtomicInteger finished = new AtomicInteger(0);

    public MysqlStreamTable(String jdbcUrl,
                               String userName,
                               String password,
                               String sql,
                               Map<String, Type> columnTypeMap) {
        this(jdbcUrl, userName, password, sql, 40000, columnTypeMap);
    }

    public MysqlStreamTable(String jdbcUrl,
                               String userName,
                               String password,
                               String sql,
                               int batchSize,
                               Map<String, Type> columnTypeMap) {
        this(Runtime.getRuntime().availableProcessors(), jdbcUrl, userName, password, sql, batchSize, columnTypeMap);
    }

    public MysqlStreamTable(int thread,
                               String jdbcUrl,
                               String userName,
                               String password,
                               String sql,
                               int batchSize,
                               Map<String, Type> columnTypeMap) {
        super(thread, columnTypeMap);
        this.url = requireNonNull(jdbcUrl);
        this.userName = requireNonNull(userName);
        this.password = requireNonNull(password);
        this.sql = requireNonNull(sql);
        this.batchSize = batchSize;
        this.myName = format("%s: %s %s", this.getClass().getSimpleName(), url, sql);
        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat(myName + "-%d").build());
    }

    @Override
    public boolean isFinished() {
        return finished.get() >= thread && super.isFinished();
    }

    @Override
    public void start() {
        int myHash = SystemProperty.getMyHash();
        int serverCount = SystemProperty.getServerCount();
        long step = serverCount * thread * batchSize;
        finished.set(0);
        for (int i = 0; i < thread; i++) {
            final int finalI = i;
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        MysqlFetcher mysqlFetcher = new MysqlFetcher(url, userName, password, columnTypeMap);
                        long start = (myHash * thread + finalI) * batchSize;
                        while (!Thread.interrupted()) {
                            Table table = mysqlFetcher.fetch(sql + " limit " + start + ", " + batchSize);
                            arrayBlockingQueueList.get(finalI).put(table);
                            if (table.size() < batchSize) {
                                break;
                            }
                            start += step;
                        }
                    } catch (InterruptedException e) {
                        logger.info("interrupted");
                    } catch (Throwable t) {
                        handleException(t);
                    } finally {
                        finished.incrementAndGet();
                    }
                }
            });
        }
    }

    @Override
    public void stop() {
        threadPoolExecutor.shutdownNow();
    }
}
