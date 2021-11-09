package com.alibaba.tc.sp.dimension;

import com.alibaba.tc.table.Index;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.TableBuilder;
import com.alibaba.tc.table.Type;
import com.alibaba.tc.exception.UnknownTypeException;
import com.taobao.tddl.group.jdbc.TGroupConnection;
import com.taobao.tddl.group.jdbc.TGroupPreparedStatement;
import com.taobao.tddl.jdbc.group.TGroupDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.tc.Threads.threadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TddlDimensionTable extends DimensionTable {
    private static final Logger logger = LoggerFactory.getLogger(TddlDimensionTable.class);

    private final String appName;
    private final String groupName;
    private final String ak;
    private final String sk;
    private final Duration refreshInterval;
    private final Map<String, Type> columnTypeMap;
    private final String[] primaryKeyColumnNames;
    private final String sql;
    private final String myName;

    public TddlDimensionTable(String appName,
                              String tableName,
                              String ak,
                              String sk,
                              Duration refreshInterval,
                              Map<String, Type> columnTypeMap,
                              String... primaryKeyColumnNames) {
        this(appName,
                appName.replaceFirst("_APP$", "_GROUP"),
                format("select %s from %s", String.join(",", columnTypeMap.keySet()), tableName),
                ak,
                sk,
                null,
                refreshInterval,
                columnTypeMap,
                primaryKeyColumnNames);
    }

    public TddlDimensionTable(String appName,
                              String groupName,
                              String sql,
                              String ak,
                              String sk,
                              String unitName,
                              Duration refreshInterval,
                              Map<String, Type> columnTypeMap,
                              String... primaryKeyColumnNames) {
        this.appName = requireNonNull(appName);
        this.groupName = requireNonNull(groupName);
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

        this.myName = format("%s-%s-%s", this.getClass().getSimpleName(), appName, sql.substring(0, sql.length() > 20 ? 20 : sql.length()));
        this.sql = requireNonNull(sql);

        new ScheduledThreadPoolExecutor(1, threadsNamed(myName)).
                scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            long pre = System.currentTimeMillis();
                            logger.info("begin to load {}", myName);
                            TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
                            TGroupDataSource tGroupDataSource = new TGroupDataSource();
                            tGroupDataSource.setDbGroupKey(groupName);
                            tGroupDataSource.setAppName(appName);
                            tGroupDataSource.setAccessKey(ak);
                            tGroupDataSource.setSecretKey(sk);
                            tGroupDataSource.setUnitName(unitName);
                            tGroupDataSource.init();
                            TGroupConnection tGroupConnection = tGroupDataSource.getConnection();
                            TGroupPreparedStatement tGroupPreparedStatement = tGroupConnection.prepareStatement(sql);
                            ResultSet resultSet = tGroupPreparedStatement.executeQuery();
                            int row = 0;
                            while (resultSet.next()) {
                                if (debug(row)) {
                                    break;
                                }

                                int i = 0;
                                for (Type type : columnTypeMap.values()) {
                                    int i1 = i + 1;
                                    switch (type) {
                                        case INT:
                                            tableBuilder.append(i, resultSet.getInt(i1));
                                            break;
                                        case BIGINT:
                                            tableBuilder.append(i, resultSet.getLong(i1));
                                            break;
                                        case DOUBLE:
                                            tableBuilder.append(i, resultSet.getDouble(i1));
                                            break;
                                        case VARCHAR:
                                            tableBuilder.append(i, resultSet.getString(i1));
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
                            resultSet.close();
                            tGroupPreparedStatement.close();
                            tGroupConnection.close();

                            Table table = tableBuilder.build();
                            Index index = table.createIndex(primaryKeyColumnNames);
                            tableIndex = new TableIndex(table, index);
                            logger.info("end to load {}, rows: {}, index.size: {}", myName, row, index.getColumns2Rows().size());
                        } catch (Throwable t) {
                            logger.error("", t);
                            try {
                                Thread.sleep(123_000);
                                run();
                            } catch (Throwable t1) {
                                logger.error("", t1);
                            }
                        }
                    }
                }, 0, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
    }
}
