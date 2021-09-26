package com.alibaba.jstream.sp.output;

import com.alibaba.jstream.offheap.ByteArray;
import com.alibaba.jstream.sp.QueueSizeLogger;
import com.alibaba.jstream.table.Column;
import com.alibaba.jstream.table.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.Threads.threadsNamed;
import static com.alibaba.jstream.sp.StreamProcessing.handleException;
import static com.alibaba.jstream.util.IpUtil.getIp;
import static com.alibaba.jstream.util.ScalarUtil.toStr;
import static java.lang.Integer.toHexString;
import static java.lang.Runtime.getRuntime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaOutputTable.class);

    private final String topic;
    private final Properties properties;
    private final int batchSize;
    private volatile int[] allPartitions;
    private final String sign;
    private final ScheduledExecutorService partitionsDetector;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final QueueSizeLogger queueSizeLogger = new QueueSizeLogger();
    protected final QueueSizeLogger recordSizeLogger = new QueueSizeLogger();
    private final Random random = new Random();

    public KafkaOutputTable(String bootstrapServers,
                            String topic) {
        this(getRuntime().availableProcessors(), 40000, bootstrapServers, topic);
    }

    public KafkaOutputTable(int thread,
                            String bootstrapServers,
                            String topic) {
        this(thread, 40000, bootstrapServers, topic);
    }

    public KafkaOutputTable(int thread,
                            int batchSize,
                            String bootstrapServers,
                            String topic) {
        super(thread);
        this.topic = requireNonNull(topic);
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, requireNonNull(bootstrapServers));
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.properties = properties;
        this.batchSize = batchSize;
        this.sign = "|KafkaOutputTable|" + topic + "|" + toHexString(hashCode());
        this.partitionsDetector = newSingleThreadScheduledExecutor(threadsNamed("partitions_detector" + sign));
        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat("kafka-output-%d").build());
    }

    @Override
    public void produce(Table table) throws InterruptedException {
        queueSizeLogger.logQueueSize("Kafka输出队列大小" + sign, arrayBlockingQueueList);
        recordSizeLogger.logRecordSize("Kafka输出队列行数" + sign, arrayBlockingQueueList);
        putTable(table);
    }

    private void detectPartitions() {
        Producer<Integer, String> producerForDetection = new KafkaProducer<>(properties);
        List<PartitionInfo> partitionInfos = producerForDetection.partitionsFor(topic);
        int[] arr = new int[partitionInfos.size()];
        int i = 0;
        for (PartitionInfo partitionInfo : partitionInfos) {
            arr[i++] = partitionInfo.partition();
        }
        allPartitions = arr;
    }

    public void start() {
        detectPartitions();
        partitionsDetector.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logger.info("{} partitions: {}", sign, allPartitions);
                detectPartitions();
            }
        }, 5, 5, TimeUnit.SECONDS);

        for (int i = 0; i < thread; i++) {
            final int finalI = i;
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Properties tmp = (Properties) properties.clone();
                    tmp.put(CLIENT_ID_CONFIG, getIp() + "-" + finalI);
                    Producer<Integer, String> producer = new KafkaProducer(tmp);
                    while (!Thread.interrupted()) {
                        try {
                            Table table = consume();
                            List<Column> columns = table.getColumns();

                            long now = System.currentTimeMillis();
                            int nowInt = (int) (now / 1000);
                            for (int i = 0; i < table.size(); i++) {
                                JsonObject jsonObject = new JsonObject();
                                for (int j = 0; j < columns.size(); j++) {
                                    if (null != columns.get(j).get(i)) {
                                        String key = columns.get(j).name();
                                        Comparable comparable = columns.get(j).get(i);
                                        if (null == comparable) {
                                            jsonObject.add(key, null);
                                            continue;
                                        }
                                        if (comparable instanceof String || comparable instanceof ByteArray) {
                                            jsonObject.addProperty(key, toStr(comparable));
                                        } else {
                                            if (__time__.equals(key)) {
                                                now = (long) comparable;
                                                nowInt = (int) (now / 1000);
                                            } else {
                                                jsonObject.addProperty(key, (Number) comparable);
                                            }
                                        }
                                    }
                                }
                                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic,
                                        allPartitions[random.nextInt(allPartitions.length)],
                                        nowInt,
                                        jsonObject.toString());
                                producer.send(producerRecord);
                                if (i > 0 && i % batchSize == 0) {
                                    producer.flush();
                                    now = System.currentTimeMillis();
                                    nowInt = (int) (now / 1000);
                                }
                            }

                            producer.flush();
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
