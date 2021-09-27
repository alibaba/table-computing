package com.alibaba.tc.sp.input;

import com.alibaba.tc.SystemProperty;
import com.alibaba.tc.offheap.ByteArray;
import com.alibaba.tc.sp.Delay;
import com.alibaba.tc.table.Column;
import com.alibaba.tc.table.TableBuilder;
import com.alibaba.tc.table.Type;
import com.alibaba.tc.util.DateUtil;
import com.alibaba.tc.util.IpUtil;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.ListShardRequest;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.log.response.ListShardResponse;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.tc.sp.StreamProcessing.handleException;
import static java.lang.Integer.toHexString;
import static java.util.Objects.requireNonNull;

public class SlsStreamTable extends AbstractStreamTable {
    private static final Logger logger = LoggerFactory.getLogger(SlsStreamTable.class);

    private final String endPoint;
    private final String accessId;
    private final String accessKey;
    private final String project;
    private final String logstore;
    private final String consumerGroup;
    private final int consumeFrom;
    private final int consumeTo;
    private final String sign;
    private long finishDelayMs = 30000;
    private long lastUpdateMs = System.currentTimeMillis();
    private final Set<Integer> shardSet = new HashSet<>();
    //由于shardSet.size()读取非常频繁且计算代价比较大使用shardSetSize缓存该值
    private int shardSetSize;

    private final List<ClientWorker> workers;

    public SlsStreamTable(String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          Map<String, Type> columnTypeMap) {
        this(1, endPoint, accessId, accessKey, project, logstore, consumerGroup, columnTypeMap);
    }

    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          Map<String, Type> columnTypeMap) {
        this(thread, endPoint, accessId, accessKey, project, logstore, consumerGroup,
                (int) (System.currentTimeMillis() / 1000),
                -1,
                columnTypeMap);
    }

    /**
     * consumeFrom example: "2021-04-09 16:16:00"
     *
     * @param thread
     * @param endPoint
     * @param accessId
     * @param accessKey
     * @param project
     * @param logstore
     * @param consumerGroup
     * @param consumeFrom
     * @param columnTypeMap
     */
    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          String consumeFrom,
                          Map<String, Type> columnTypeMap) throws ParseException {
        this(thread, endPoint, accessId, accessKey, project, logstore, consumerGroup, (int) (DateUtil.parseDate(consumeFrom) / 1000), -1, columnTypeMap);
    }

    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          String consumeFrom,
                          String consumeTo,
                          Map<String, Type> columnTypeMap) throws ParseException {
        this(thread, endPoint, accessId, accessKey, project, logstore, consumerGroup,
                (int) (DateUtil.parseDate(consumeFrom) / 1000),
                (int) (DateUtil.parseDate(consumeTo) / 1000),
                columnTypeMap);
    }

    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          int consumeFrom,
                          int consumeTo,
                          Map<String, Type> columnTypeMap) {
        super(thread, columnTypeMap);

        this.endPoint = requireNonNull(endPoint);
        this.accessId = requireNonNull(accessId);
        this.accessKey = requireNonNull(accessKey);
        this.project = requireNonNull(project);
        this.logstore = requireNonNull(logstore);
        this.consumerGroup = requireNonNull(consumerGroup);
        this.consumeFrom = consumeFrom;
        this.consumeTo = consumeTo;
        this.sign = "|SlsStreamTable|" + project + "|" + logstore + "|" + toHexString(hashCode());

        workers = new ArrayList<>(thread);
    }

    /**
     * 使用synchronized确保该值set之后其它线程可见
     * sls自动分裂期间会导致shard的变化，通过一个延时确定在这个延时内shard没有变化且每个shard都已经消费到consumeTo了
     * default: 30秒
     * @param finishDelay
     */
    public synchronized void setFinishDelaySeconds(Duration finishDelay) {
        this.finishDelayMs = finishDelay.toMillis();
    }

    private synchronized void addShard(int shardId) {
        shardSet.add(shardId);
        shardSetSize = shardSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    private synchronized void removeShard(int shardId) {
        shardSet.remove(shardId);
        shardSetSize = shardSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    private class LogHubProcessor implements ILogHubProcessor {
        private final int threadId;
        private final SlsStreamTable slsStreamTable;

        private int shardId;
        // 记录上次持久化Checkpoint的时间。
        private long mLastCheckTime = 0;

        LogHubProcessor(int threadId, SlsStreamTable slsStreamTable) {
            this.threadId = threadId;
            this.slsStreamTable = slsStreamTable;
        }

        @Override
        public void initialize(int shardId) {
            this.shardId = shardId;
            slsStreamTable.addShard(shardId);
        }

        // 消费数据的主逻辑，消费时的所有异常都需要处理，不能直接抛出。
        @Override
        public String process(List<LogGroupData> logGroups, ILogHubCheckPointTracker checkPointTracker) {
            try {
                if (!slsStreamTable.shardSet.contains(shardId)) {
                    return null;
                }
                TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
                Map<ByteArray, ByteArray> tmp = new HashMap<>();
                for (LogGroupData logGroup : logGroups) {
                    new SlsParser(logGroup, new SlsParser.Callback() {
                        private int size = 0;

                        @Override
                        public void keyValue(byte[] rawBytes, int keyOffset, int keyLength, int valueOffset, int valueLength) {
                            if (-1 == keyOffset || -1 == valueOffset) {
                                return;
                            }

                            ByteArray key = new ByteArray(rawBytes, keyOffset, keyLength);

                            //下面这段是逆优化，会使性能更差
//                            if (!columnNames.contains(key)) {
//                                return;
//                            }

                            tmp.put(key, new ByteArray(rawBytes, valueOffset, valueLength));
                        }

                        @Override
                        public void nextLog(int time) {
                            if (tmp.isEmpty()) {
                                return;
                            }
                            if (-1 != consumeTo && time >= consumeTo) {
                                slsStreamTable.removeShard(shardId);
                                return;
                            }

                            long now = System.currentTimeMillis();
                            long ms = (long) time * 1000;
                            Delay.DELAY.log("业务延迟" + slsStreamTable.sign, ms);
                            Delay.DELAY.log("数据间隔" + slsStreamTable.sign, now);
                            Delay.RESIDENCE_TIME.log("数据滞留" + slsStreamTable.sign, now - ms);

                            int i = 0;
                            for (ByteArray key : columns) {
                                if (key == __machine_uuid__ ||
                                        key == __category__ ||
                                        key == __source__ ||
                                        key == __topic__) {
                                    i++;
                                    continue;
                                }
                                if (key == __time__) {
                                    tableBuilder.append(i++, ms);
                                } else {
                                    tableBuilder.append(i++, tmp.get(key));
                                }
                            }
                            size++;
                            tmp.clear();
                        }

                        private void addExtraColumn(ByteArray columnName, ByteArray value) {
                            if (columnNames.contains(columnName)) {
                                Column column = tableBuilder.getColumn(columnName.toString());
                                for (int i = 0; i < size; i++) {
                                    column.add(value);
                                }
                            }
                        }

                        @Override
                        public void end(ByteArray category, ByteArray topic, ByteArray source, ByteArray machineUUID) {
                            addExtraColumn(__category__, category);
                            addExtraColumn(__topic__, topic);
                            addExtraColumn(__source__, source);
                            addExtraColumn(__machine_uuid__, machineUUID);
                        }
                    });
                }

                queueSizeLogger.logQueueSize("输入队列大小" + slsStreamTable.sign, arrayBlockingQueueList);
                recordSizeLogger.logRecordSize("输入队列行数" + slsStreamTable.sign, arrayBlockingQueueList);
                arrayBlockingQueueList.get(threadId).put(tableBuilder.build());

                return null;
            } catch (Throwable t) {
                handleException(t);
                return null;
            } finally {
                //下面这段代码放到finally里执行确保提前返回的情况下也有机会saveCheckPoint到服务端，否则新分裂出来的shard由于分裂前的shard
                //一直没收到check point会认为之前的shard一直没消费完而不会分配新分裂出来的shard进行消费
                long curTime = System.currentTimeMillis();
                // 每隔90秒，写一次Checkpoint到服务端。如果90秒内发生Worker异常终止，新启动的Worker会从上一个Checkpoint获取消费数据，可能存在少量的重复数据。
                if (curTime - mLastCheckTime > 90 * 1000) {
                    try {
                        //参数为true表示立即将Checkpoint更新到服务端；false表示将Checkpoint缓存在本地，默认间隔60秒会将Checkpoint更新到服务端。
                        checkPointTracker.saveCheckPoint(true);
                    } catch (Throwable t) {
                        handleException(t);
                    }
                    mLastCheckTime = curTime;
                }
            }
        }

        // 当Worker退出时，会调用该函数，您可以在此处执行清理工作。
        @Override
        public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
            //将Checkpoint立即保存到服务端。
            try {
                slsStreamTable.removeShard(shardId);
                checkPointTracker.saveCheckPoint(true);
            } catch (Throwable t) {
                handleException(t);
            }
        }
    }

    private class LogHubProcessorFactory implements ILogHubProcessorFactory {
        private final int threadId;
        private final SlsStreamTable slsStreamTable;

        LogHubProcessorFactory(int threadId, SlsStreamTable slsStreamTable) {
            this.threadId = threadId;
            this.slsStreamTable = slsStreamTable;
        }

        @Override
        public ILogHubProcessor generatorProcessor() {
            // 生成一个消费实例。
            return new LogHubProcessor(threadId, slsStreamTable);
        }
    }

    private void updateCheckpoint(Client client, long timestamp) throws LogException, InterruptedException {
        try {
//        long timestamp = Timestamp.valueOf("2017-11-15 00:00:00").getTime() / 1000;
            ListShardResponse response = client.ListShard(new ListShardRequest(project, logstore));
            for (Shard shard : response.GetShards()) {
                int shardId = shard.GetShardId();
                String cursor = client.GetCursor(project, logstore, shardId, timestamp).GetCursor();
                client.UpdateCheckPoint(project, logstore, consumerGroup, shardId, cursor);
                logger.info("update shardId: {}, cursor: {}", shardId, cursor);
            }
        } catch (LogException e) {
            if (!"shard not exist".equals(e.getMessage())) {
                throw e;
            }
            // 第一次创建consumer group后马上updateCheckPoint会报shard not exist异常需要持续等几秒等SLS服务端将consumer group下的
            // shard创建好之后才可以
            Thread.sleep(1000);
            updateCheckpoint(client, timestamp);
        }
    }

    private ConsumerGroup getConsumerGroup(Client slsClient) throws LogException {
        ListConsumerGroupResponse response;
        response = slsClient.ListConsumerGroup(this.project, this.logstore);
        if (response != null) {
            Iterator iterator = response.GetConsumerGroups().iterator();

            while (iterator.hasNext()) {
                ConsumerGroup item = (ConsumerGroup) iterator.next();
                if (item.getConsumerGroupName().equalsIgnoreCase(this.consumerGroup)) {
                    return item;
                }
            }
        }

        return null;
    }

    @Override
    public void start() {
        try {
            long heartbeatIntervalMillis = 5000;
            //连续10次心跳检测失败则删掉对应的consumer分配给别的consumer继续消费
            int timeout = (int) (heartbeatIntervalMillis * 10 / 1000);
            //保序，否则时间差距会太大watermark也很难保序
            boolean consumeInOrder = true;
            Client slsClient = new Client(endPoint, accessId, accessKey);
            if (null == getConsumerGroup(slsClient)) {
                slsClient.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroup, timeout, consumeInOrder));
            }

            updateCheckpoint(slsClient, consumeFrom);

            for (int i = 0; i < thread; i++) {
                // consumer_是消费者名称，同一个消费组下面的消费者名称必须不同，不同的消费者名称在多台机器上启动多个进程，来均衡消费一个Logstore，
                // 此时消费者名称可以使用机器IP地址来区分。maxFetchLogGroupSize是每次从服务端获取的LogGroup最大数目，使用默认值即可，
                // 如有调整请注意取值范围(0,1000]。
                LogHubConfig config = new LogHubConfig(consumerGroup,
                        "consumer_" + IpUtil.getIp() + "_" + SystemProperty.mySign() + "_" + i,
                        endPoint,
                        project,
                        logstore,
                        accessId,
                        accessKey,
                        consumeFrom);
                config.setHeartBeatIntervalMillis(heartbeatIntervalMillis);
                config.setTimeoutInSeconds(timeout);
                config.setConsumeInOrder(consumeInOrder);
                config.setMaxFetchLogGroupSize(1000);
                ClientWorker worker = new ClientWorker(new LogHubProcessorFactory(i, this), config);

                workers.add(worker);
                Thread thread = new Thread(worker, "sls-consumer-" + i);
                //Thread运行之后，ClientWorker会自动运行，ClientWorker扩展了Runnable接口。
                thread.start();
            }
        } catch (LogException | InterruptedException | LogHubClientWorkerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isFinished() {
        if (-1 == consumeTo) {
            return false;
        }
        if (shardSetSize <= 0 && System.currentTimeMillis() - lastUpdateMs >= finishDelayMs) {
            return true;
        }
        return false;
    }

    @Override
    public void stop() {
        try {
            for (ClientWorker worker : workers) {
                //调用Worker的Shutdown函数，退出消费实例，关联的线程也会自动停止。
                worker.shutdown();
            }

            //ClientWorker运行过程中会生成多个异步的任务，Shutdown完成后请等待还在执行的任务安全退出，建议sleep配置为30秒。
            Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
            logger.info("interrupted");
        }
    }
}
