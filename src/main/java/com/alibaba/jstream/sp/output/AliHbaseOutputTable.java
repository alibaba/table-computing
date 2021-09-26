package com.alibaba.jstream.sp.output;

import com.alibaba.jstream.offheap.ByteArray;
import com.alibaba.jstream.sp.QueueSizeLogger;
import com.alibaba.jstream.table.Column;
import com.alibaba.jstream.table.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.sp.StreamProcessing.handleException;
import static java.lang.Integer.toHexString;
import static java.util.Objects.requireNonNull;

//todo: 增强单机写入能力，目前使用netstat -nap看单机跟每个region server只会建一个连接不能增大连接数提升写入能力只能通过增加机器来提升写入能力
public class AliHbaseOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(AliHbaseOutputTable.class);

    private final String diamondKey;
    private final String diamondGroup;
    private final String tableName;
    private final byte[] columnFamily;
    private final String rowKey;
    private final int batchSize;
    private final String sign;

    private final ThreadPoolExecutor threadPoolExecutor;
    private final QueueSizeLogger queueSizeLogger = new QueueSizeLogger();
    protected final QueueSizeLogger recordSizeLogger = new QueueSizeLogger();
    private final Configuration conf;

    public AliHbaseOutputTable(String diamondKey,
                               String diamondGroup,
                               String tableName,
                               String columnFamily,
                               String rowKey) {
        this(Runtime.getRuntime().availableProcessors(), 40000, diamondKey, diamondGroup, tableName, columnFamily, rowKey);
    }

    public AliHbaseOutputTable(int thread,
                               int batchSize,
                               String diamondKey,
                               String diamondGroup,
                               String tableName,
                               String columnFamily,
                               String rowKey) {
        super(thread);
        this.diamondKey = requireNonNull(diamondKey);
        this.diamondGroup = requireNonNull(diamondGroup);
        this.tableName = requireNonNull(tableName);
        this.columnFamily = requireNonNull(columnFamily).getBytes();
        this.rowKey = requireNonNull(rowKey);
        this.batchSize = batchSize;
        this.sign = "|HbaseOutputTable|" + tableName + "|" + toHexString(hashCode());

        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat("hbase-output-%d").build());

        conf = new Configuration();
        conf.set("diamond.hbase.key", diamondKey);
        conf.set("hbase.diamond.group", diamondGroup);
        conf.set("hbase.cluster.unitized", "true");
        conf.set("hbase.client.write.buffer", "200097152");
        conf.set("hbase.client.scanner.caching", "200");
        conf.set("hbase.client.retries.number", "3");
        conf.set("hbase.rpc.timeout", "20000");

        //limit on concurrent client-side zookeeper connections
        conf.set("hbase.zookeeper.property.maxClientCnxns", "1000");

        //General client pause value. Used mostly as value to wait before running a retry of a failed get, region lookup, etc.
        //Type: long
        //Default: 1000 (1 sec)
        //Unit: milliseconds
        conf.set("hbase.client.pause", "10000");
    }

    @Override
    public void produce(Table table) throws InterruptedException {
        queueSizeLogger.logQueueSize("Hbase输出队列大小" + sign, arrayBlockingQueueList);
        recordSizeLogger.logRecordSize("Hbase输出队列行数" + sign, arrayBlockingQueueList);
        putTable(table);
    }

    public void start() {
        for (int i = 0; i < thread; i++) {
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    HTablePool hTablePool = new HTablePool(conf, 1000);
                    HTableInterface hTable = hTablePool.getTable(tableName);

                    while (!Thread.interrupted()) {
                        try {
                            Table table = consume();
                            List<Column> columns = table.getColumns();
                            Column rkColumn = table.getColumn(rowKey);
                            columns.remove(rkColumn);

                            List<Put> puts = new ArrayList<>();
                            for (int i = 0; i < table.size(); i++) {
                                byte[] rk = ((ByteArray) rkColumn.get(i)).getBytes();
                                Put put = new Put(rk);
                                for (int j = 0; j < columns.size(); j++) {
                                    if (null != columns.get(j).get(i)) {
                                        put.add(columns.get(j).name().getBytes(),
                                                columnFamily,
                                                ((ByteArray) columns.get(j).get(i)).getBytes());
                                    } else {
                                        put.add(columns.get(j).name().getBytes(),
                                                columnFamily,
                                                null);
                                    }
                                }
                                puts.add(put);
                                if (puts.size() == batchSize) {
                                    hTable.put(puts);
                                    puts.clear();
                                }
                            }

                            if (puts.size() > 0) {
                                hTable.put(puts);
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
