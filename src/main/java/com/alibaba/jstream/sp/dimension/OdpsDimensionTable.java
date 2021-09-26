package com.alibaba.jstream.sp.dimension;

import com.alibaba.jstream.exception.UnknownTypeException;
import com.alibaba.jstream.table.Index;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.TableBuilder;
import com.alibaba.jstream.table.Type;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.Threads.threadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OdpsDimensionTable extends DimensionTable {
    private static final Logger logger = LoggerFactory.getLogger(OdpsDimensionTable.class);

    private final String useProject;
    private final String projectName;
    private final String tableName;
    private final String partitionColumnName;
    private final String partition;
    private final String ak;
    private final String sk;
    private final Duration refreshInterval;
    private final Map<String, Type> columnTypeMap;
    private final String[] primaryKeyColumnNames;
    private final String myName;

    public OdpsDimensionTable(String endPoint,
                              String projectName,
                              String tableName,
                              String partitionColumnName,
                              String partition,
                              String ak,
                              String sk,
                              Duration refreshInterval,
                              Map<String, Type> columnTypeMap,
                              String... primaryKeyColumnNames) {
        this(endPoint, projectName, projectName, tableName, partitionColumnName, partition, ak, sk, refreshInterval,
                columnTypeMap, primaryKeyColumnNames);
    }

    public OdpsDimensionTable(String endPoint,
                              final String useProject,
                              String projectName,
                              String tableName,
                              String partitionColumnName,
                              String partition,
                              String ak,
                              String sk,
                              Duration refreshInterval,
                              Map<String, Type> columnTypeMap,
                              String... primaryKeyColumnNames) {
        this.useProject = requireNonNull(useProject);
        this.projectName = requireNonNull(projectName);
        this.tableName = requireNonNull(tableName);
        this.partitionColumnName = requireNonNull(partitionColumnName);
        this.partition = requireNonNull(partition).replace(" ", "").toLowerCase();
        this.ak = requireNonNull(ak);
        this.sk = requireNonNull(sk);
        this.refreshInterval = requireNonNull(refreshInterval);
        this.columnTypeMap = requireNonNull(columnTypeMap);
        if (columnTypeMap.size() < 1) {
            throw new IllegalArgumentException();
        }
        this.primaryKeyColumnNames = requireNonNull(primaryKeyColumnNames);
        if (primaryKeyColumnNames.length < 1) {
            throw new IllegalArgumentException();
        }

        this.myName = format("%s: %s.%s", this.getClass().getSimpleName(), projectName, tableName);

        new ScheduledThreadPoolExecutor(1, threadsNamed(myName)).
                scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            long pre = System.currentTimeMillis();
                            logger.info("begin to load {}", myName);

                            TableBuilder tableBuilder = new TableBuilder(columnTypeMap);

                            Account account = new AliyunAccount(ak, sk);
                            Odps odps = new Odps(account);
                            odps.setEndpoint(endPoint);
                            odps.setDefaultProject(useProject);
                            String ptSpec = partition;
                            if (partition.equals("max_pt()")) {
                                ptSpec = getMaxPtSpec(odps, projectName, tableName);
                            }

                            TableTunnel tunnel = new TableTunnel(odps);
                            TableTunnel.DownloadSession downloadSession = tunnel.createDownloadSession(projectName,
                                    tableName,
                                    new PartitionSpec(format("%s='%s'", partitionColumnName, ptSpec)));
                            long recordNum = downloadSession.getRecordCount();
                            logger.info("{} will download {} records", myName, recordNum);
                            RecordReader recordReader = downloadSession.openRecordReader(0, recordNum);
                            Record record;
                            int row = 0;
                            while ((record = recordReader.read()) != null) {
                                if (debug(row)) {
                                    break;
                                }

                                int i = 0;
                                for (String columnName : columnTypeMap.keySet()) {
                                    Type type = columnTypeMap.get(columnName);
                                    switch (type) {
                                        case INT:
                                            tableBuilder.append(i, record.getBigint(columnName) == null ? null : record.getBigint(columnName).intValue());
                                            break;
                                        case BIGINT:
                                            tableBuilder.append(i, record.getBigint(columnName));
                                            break;
                                        case DOUBLE:
                                            tableBuilder.append(i, record.getDouble(columnName));
                                            break;
                                        case VARCHAR:
                                            tableBuilder.append(i, record.getString(columnName));
                                            break;
                                        default:
                                            throw new UnknownTypeException(type.name());
                                    }
                                    i++;
                                }
                                row++;

                                long now = System.currentTimeMillis();
                                if (now - pre > 5000) {
                                    logger.info("{} have loaded {} rows", myName, row);
                                    pre = now;
                                }
                            }

                            Table table = tableBuilder.build();
                            Index index = table.createIndex(primaryKeyColumnNames);
                            tableIndex = new TableIndex(table, index);
                            logger.info("end to load {}, rows: {}, index.size: {}", myName, row, index.getColumns2Rows().size());
                        } catch (Throwable t) {
                            logger.error("", t);
                            try {
                                Thread.sleep(10_000);
                                run();
                            } catch (Throwable t1) {
                                logger.error("", t1);
                            }
                        }
                    }
                }, 0, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static String getMaxPtSpec(Odps odps, String projectName, String tableName) throws OdpsException {
        Instance instance = SQLTask.run(odps, "select max_pt('" + projectName + "." + tableName + "');");
        instance.waitForSuccess();
        List<Record> records = SQLTask.getResult(instance);
        return records.get(0).getString(0);
    }

    private static String getMaxPtSpecWithoutSql(Odps odps, String projectName, String tableName) {
        odps.setDefaultProject(projectName);
        List<Partition> partitions = odps.tables().get(tableName).getPartitions();
        String maxPtSpec = null;
        for(Partition partition : partitions) {
            PartitionSpec partitionSpec = partition.getPartitionSpec();
            String ptSpec = partitionSpec.get(partitionSpec.keys().iterator().next());
            if (null == maxPtSpec || maxPtSpec.compareTo(ptSpec) < 0) {
                maxPtSpec = ptSpec;
            }
        }
        return maxPtSpec;
    }
}
