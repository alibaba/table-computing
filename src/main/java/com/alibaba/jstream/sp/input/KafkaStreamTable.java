package com.alibaba.jstream.sp.input;

import com.alibaba.jstream.exception.UnknownTypeException;
import com.alibaba.jstream.offheap.ByteArray;
import com.alibaba.jstream.table.TableBuilder;
import com.alibaba.jstream.table.Type;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.SystemProperty.getMyHash;
import static com.alibaba.jstream.SystemProperty.getServerCount;
import static com.alibaba.jstream.Threads.threadsNamed;
import static com.alibaba.jstream.sp.input.kafka.MyKafkaConsumer.newKafkaConsumer;
import static java.lang.Integer.toHexString;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.codehaus.groovy.runtime.InvokerHelper.asList;

public class KafkaStreamTable extends AbstractStreamTable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamTable.class);

    private final Properties properties;
    private final String topic;
    private final String sign;
    private final long consumeFrom;
    private final long consumeTo;
    private final int myHash;
    private final int serverCount;
    private final Set<Integer> myPartitions = new HashSet<>();
    private final ScheduledExecutorService partitionsDetector;
    private final List<Thread> consumers = new ArrayList<>();
    private final int timeColumnIndex;
    private final List<String> stringColumns;
    private final List<Type> types;
    private long finishDelayMs = 30000;
    private long lastUpdateMs = System.currentTimeMillis();
    private final Set<Integer> partitionSet = new HashSet<>();
    //由于partitionSet.size()读取非常频繁且计算代价比较大使用partitionSetSize缓存该值
    private int partitionSetSize;

    public KafkaStreamTable(String bootstrapServers,
                            String consumerGroupId,
                            String topic,
                            long consumeFrom,
                            Map<String, Type> columnTypeMap) {
        this(bootstrapServers, consumerGroupId, topic, consumeFrom, -1, columnTypeMap);
    }

    /**
     * @param bootstrapServers
     * @param consumerGroupId
     * @param topic
     * @param consumeFrom
     * @param consumeTo
     * @param columnTypeMap
     */
    public KafkaStreamTable(String bootstrapServers,
                            String consumerGroupId,
                            String topic,
                            long consumeFrom,
                            long consumeTo,
                            Map<String, Type> columnTypeMap) {
        super(0, columnTypeMap);
        this.topic = requireNonNull(topic);
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, requireNonNull(bootstrapServers));
        properties.put(GROUP_ID_CONFIG, requireNonNull(consumerGroupId));
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(MAX_POLL_RECORDS_CONFIG, 40000);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(AUTO_OFFSET_RESET_CONFIG, "none");

        this.properties = properties;
        this.sign = "|KafkaStreamTable|" + topic + "|" + toHexString(hashCode());
        this.partitionsDetector = newSingleThreadScheduledExecutor(threadsNamed("partitions_detector" + sign));
        this.consumeFrom = consumeFrom;
        this.consumeTo = consumeTo;
        myHash = getMyHash();
        serverCount = getServerCount();
        stringColumns = new ArrayList<>(columns.size());
        types = new ArrayList<>(columns.size());
        timeColumnIndex = columns.indexOf(__time__);
        for (ByteArray column : columns) {
            String columnName = column.toString();
            stringColumns.add(columnName);
            types.add(columnTypeMap.get(columnName));
        }
    }

    private void newConsumer(TopicPartition topicPartition, OffsetAndTimestamp offsetAndTimestamp) {
        if (topicPartition.partition() % serverCount != myHash) {
            return;
        }
        if (myPartitions.contains(topicPartition.partition())) {
            return;
        }
        myPartitions.add(topicPartition.partition());
        addPartition(topicPartition.partition());
        int threadId = arrayBlockingQueueList.size();
        arrayBlockingQueueList.add(new ArrayBlockingQueue<>(queueDepth));
        KafkaStreamTable kafkaStreamTable = this;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Consumer<Integer, String> consumer = new KafkaConsumer<>(properties);
                consumer.assign(asList(topicPartition));
                if (null == offsetAndTimestamp) {
                    consumer.seekToBeginning(asList(topicPartition));
                } else {
                    consumer.seek(topicPartition, offsetAndTimestamp.offset());
                }

                Gson gson = new Gson();
                while (!Thread.interrupted()) {
                    try {
                        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(sleepMs));
                        if (records.isEmpty()) {
                            continue;
                        }
                        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
                        for (ConsumerRecord<Integer, String> record : records) {
                            Integer time = record.key();
                            if (-1 != consumeTo && time >= consumeTo) {
                                kafkaStreamTable.removePartition(topicPartition.partition());
                                return;
                            }
                            String value = record.value();
                            JsonObject jsonObject = gson.fromJson(value, JsonObject.class);
                            for (int i = 0; i < stringColumns.size(); i++) {
                                if (i == timeColumnIndex) {
                                    tableBuilder.append(i, time * 1000L);
                                } else {
                                    JsonElement jsonElement = jsonObject.get(stringColumns.get(i));
                                    if (null == jsonElement || jsonElement.isJsonNull()) {
                                        tableBuilder.appendValue(i, null);
                                    } else {
                                        Type type = types.get(i);
                                        switch (type) {
                                            case DOUBLE:
                                                tableBuilder.append(i, jsonElement.getAsDouble());
                                                break;
                                            case BIGINT:
                                                tableBuilder.append(i, jsonElement.getAsLong());
                                                break;
                                            case INT:
                                                tableBuilder.append(i, jsonElement.getAsInt());
                                                break;
                                            case VARCHAR:
                                                tableBuilder.append(i, jsonElement.getAsString());
                                                break;
                                            default:
                                                throw new UnknownTypeException(type.name());
                                        }
                                    }
                                }
                            }
                        }
                        queueSizeLogger.logQueueSize("input queue size" + kafkaStreamTable.sign, arrayBlockingQueueList);
                        recordSizeLogger.logRecordSize("input queue rows" + kafkaStreamTable.sign, arrayBlockingQueueList);
                        arrayBlockingQueueList.get(threadId).put(tableBuilder.build());
                    } catch (InterruptException e) {
                        break;
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });
        thread.start();
        consumers.add(thread);
    }

    private synchronized void addPartition(int partition) {
        partitionSet.add(partition);
        partitionSetSize = partitionSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    private synchronized void removePartition(int partition) {
        partitionSet.remove(partition);
        partitionSetSize = partitionSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    @Override
    public boolean isFinished() {
        if (-1 == consumeTo) {
            return false;
        }
        if (partitionSetSize <= 0 && System.currentTimeMillis() - lastUpdateMs >= finishDelayMs) {
            return true;
        }
        return false;
    }

    @Override
    public void start() {
        Consumer<String, String> consumer = newKafkaConsumer(properties);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Map<TopicPartition, Long> topicPartitionTimes = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            topicPartitionTimes.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), consumeFrom);
        }
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsets = consumer.offsetsForTimes(topicPartitionTimes);
        for (TopicPartition topicPartition : topicPartitionOffsets.keySet()) {
            newConsumer(topicPartition, topicPartitionOffsets.get(topicPartition));
        }

        partitionsDetector.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logger.info("{} partitions: {}", sign, myPartitions);
                Consumer<String, String> consumer = newKafkaConsumer(properties);
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    newConsumer(new TopicPartition(topic, partitionInfo.partition()), null);
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        partitionsDetector.shutdownNow();
        for (Thread consumer : consumers) {
            consumer.interrupt();
        }
        consumers.clear();
        myPartitions.clear();
        partitionSet.clear();
        partitionSetSize = 0;
    }
}
