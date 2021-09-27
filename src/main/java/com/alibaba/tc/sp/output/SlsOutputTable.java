package com.alibaba.tc.sp.output;

import com.alibaba.tc.sp.QueueSizeLogger;
import com.alibaba.tc.table.Column;
import com.alibaba.tc.table.Table;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.tc.sp.StreamProcessing.handleException;
import static com.alibaba.tc.util.ScalarUtil.toStr;
import static java.lang.Integer.toHexString;
import static java.util.Objects.requireNonNull;

public class SlsOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(SlsOutputTable.class);

    private final String endPoint;
    private final String accessId;
    private final String accessKey;
    private final String project;
    private final String logstore;
    private final int batchSize;
    private final String sign;

    private final ThreadPoolExecutor threadPoolExecutor;
    private final QueueSizeLogger queueSizeLogger = new QueueSizeLogger();
    protected final QueueSizeLogger recordSizeLogger = new QueueSizeLogger();

    public SlsOutputTable(String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore) {
        this(Runtime.getRuntime().availableProcessors(), 40000, endPoint, accessId, accessKey, project, logstore);
    }

    public SlsOutputTable(int thread,
                          int batchSize,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore) {
        super(thread);
        this.endPoint = requireNonNull(endPoint);
        this.accessId = requireNonNull(accessId);
        this.accessKey = requireNonNull(accessKey);
        this.project = requireNonNull(project);
        this.logstore = requireNonNull(logstore);
        this.batchSize = batchSize;
        this.sign = "|SlsOutputTable|" + project + "|" + logstore + "|" + toHexString(hashCode());

        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat("sls-output-%d").build());
    }

    @Override
    public void produce(Table table) throws InterruptedException {
        queueSizeLogger.logQueueSize("Sls输出队列大小" + sign, arrayBlockingQueueList);
        recordSizeLogger.logRecordSize("Sls输出队列行数" + sign, arrayBlockingQueueList);
        putTable(table);
    }

    public void start() {
        for (int i = 0; i < thread; i++) {
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Client slsClient = new Client(endPoint, accessId, accessKey);

                    while (!Thread.interrupted()) {
                        try {
                            Table table = consume();
                            List<Column> columns = table.getColumns();

                            List<LogItem> logItems = new ArrayList<>();
                            for (int i = 0; i < table.size(); i++) {
                                LogItem logItem = new LogItem();
                                for (int j = 0; j < columns.size(); j++) {
                                    if (null != columns.get(j).get(i)) {
                                        String key = columns.get(j).name();
                                        Comparable value = columns.get(j).get(i);
                                        if (__time__.equals(key)) {
                                            logItem.SetTime((int) (((long) value) / 1000));
                                        } else {
                                            logItem.PushBack(key, toStr(value));
                                        }
                                    }
                                }
                                logItems.add(logItem);
                                if (logItems.size() == batchSize) {
                                    slsClient.PutLogs(project, logstore, "", logItems, "");
                                    logItems.clear();
                                }
                            }

                            if (logItems.size() > 0) {
                                slsClient.PutLogs(project, logstore, "", logItems, "");
                            }
                        } catch (InterruptedException e) {
                            logger.info("interrupted");
                            break;
                        } catch (Throwable t) {
                            handleException(t);
                            break;
                        }
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
