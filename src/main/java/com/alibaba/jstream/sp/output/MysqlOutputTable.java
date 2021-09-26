package com.alibaba.jstream.sp.output;

import com.alibaba.jstream.exception.InconsistentColumnSizeException;
import com.alibaba.jstream.exception.UnknownTypeException;
import com.alibaba.jstream.sp.QueueSizeLogger;
import com.alibaba.jstream.table.Column;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.Type;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.jstream.sp.StreamProcessing.handleException;
import static com.alibaba.jstream.util.ScalarUtil.toDouble;
import static com.alibaba.jstream.util.ScalarUtil.toInteger;
import static com.alibaba.jstream.util.ScalarUtil.toLong;
import static com.alibaba.jstream.util.ScalarUtil.toStr;
import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MysqlOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(MysqlOutputTable.class);

    private final String jdbcUrl;
    private final String tableName;
    private final String userName;
    private final String password;
    private final String createIndex;
    private final int maxRetryTimes;
    private final int batchSize;
    private final boolean autoDropTable;
    protected final Map<String, Type> columnTypeMap;
    private final String sign;
    private final String insertPrefix;

    private final ThreadPoolExecutor threadPoolExecutor;
    private final QueueSizeLogger queueSizeLogger = new QueueSizeLogger();
    protected final QueueSizeLogger recordSizeLogger = new QueueSizeLogger();

    public MysqlOutputTable(String jdbcUrl,
                            String userName,
                            String password,
                            String tableName,
                            String createIndex,
                            boolean autoDropTable,
                            Map<String, Type> columnTypeMap) throws IOException {
        this(Runtime.getRuntime().availableProcessors(),
                1000,
                jdbcUrl,
                userName,
                password,
                tableName,
                createIndex,
                1,
                autoDropTable,
                columnTypeMap);
    }

    public MysqlOutputTable(int thread,
                            int batchSize,
                            String jdbcUrl,
                            String userName,
                            String password,
                            String tableName,
                            String createIndex,
                            int maxRetryTimes,
                            boolean autoDropTable,
                            Map<String, Type> columnTypeMap) throws IOException {
        super(thread);
        this.jdbcUrl = requireNonNull(jdbcUrl);
        this.userName = requireNonNull(userName);
        this.password = requireNonNull(password);
        this.tableName = requireNonNull(tableName);
        this.createIndex = createIndex;
        this.maxRetryTimes = requireNonNull(maxRetryTimes);
        this.batchSize = batchSize;
        this.autoDropTable = autoDropTable;
        this.columnTypeMap = requireNonNull(columnTypeMap);
        this.sign = "|MysqlOutputTable|" + tableName + "|" + toHexString(hashCode());
        this.insertPrefix = "insert into " + tableName + " values ";

        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat("mysql-output-%d").build());

        createTable();
    }

    private Connection connect() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setUser(userName);
        dataSource.setPassword(password);
        dataSource.setAutoReconnect(true);
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTable() throws IOException {
        StringBuilder fieldsBuilder = new StringBuilder();
        for (String columnName : columnTypeMap.keySet()) {
            Type type = columnTypeMap.get(columnName);
            if (type == Type.VARCHAR) {
                fieldsBuilder.append(columnName).append(" ").append("longtext").append(",");
            } else {
                fieldsBuilder.append(columnName).append(" ").append(type).append(",");
            }
        }

        String fieldsSchema = fieldsBuilder.toString();
        if (fieldsSchema.length() > 0) {
            fieldsSchema = fieldsSchema.substring(0, fieldsSchema.length() - 1);
        }

        /* 表不存在则创建表 */
        String createTableSql = format("CREATE TABLE IF NOT EXISTS %s (%s) ",
                tableName, fieldsSchema);

        logger.info(">>> create table sql: " + createTableSql);

        int retryCount = 0;
        while (retryCount < maxRetryTimes) {
            try {
                Connection connection = connect();
                if (autoDropTable) {
                    PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE IF EXISTS " + tableName);
                    preparedStatement.execute();
                }
                PreparedStatement preparedStatement = connection.prepareStatement(createTableSql);
                preparedStatement.execute();
                if (!Strings.isNullOrEmpty(createIndex)) {
                    preparedStatement = connection.prepareStatement(createIndex);
                    preparedStatement.execute();
                }

                return;
            } catch (Throwable t) {
                logger.error(">>> create table error: {}, has retried {} times", Throwables.getStackTraceAsString(t), retryCount);
                retryCount++;
                if (retryCount >= maxRetryTimes) {
                    throw new IOException(t);
                }
                try {
                    Thread.sleep(1 * 1000L);
                } catch (Throwable t2) {
                    logger.error("retry sleep error!", t2);
                }
            }
        }

        throw new IOException(">>> create mysql table error for " + tableName + ", we have tried " + maxRetryTimes + " times");
    }

    @Override
    public void produce(Table table) throws InterruptedException {
        queueSizeLogger.logQueueSize("Mysql输出队列大小" + sign, arrayBlockingQueueList);
        recordSizeLogger.logRecordSize("Mysql输出队列行数" + sign, arrayBlockingQueueList);
        putTable(table);
    }

    private void setValues(PreparedStatement preparedStatement, List<Object> objectList) throws SQLException {
        for (int i = 0; i < objectList.size(); ) {
            for (Type type : columnTypeMap.values()) {
                Object object = objectList.get(i);
                switch (type) {
                    case VARCHAR:
                        if (null == object) {
                            preparedStatement.setNull(i + 1, Types.VARCHAR);
                            break;
                        }
                        preparedStatement.setString(i + 1, toStr(objectList.get(i)));
                        break;
                    case INT:
                        if (null == object) {
                            preparedStatement.setNull(i + 1, Types.INTEGER);
                            break;
                        }
                        preparedStatement.setInt(i + 1, toInteger(objectList.get(i)));
                        break;
                    case BIGINT:
                        if (null == object) {
                            preparedStatement.setNull(i + 1, Types.BIGINT);
                            break;
                        }
                        preparedStatement.setLong(i + 1, toLong(objectList.get(i)));
                        break;
                    case DOUBLE:
                        if (null == object) {
                            preparedStatement.setNull(i + 1, Types.DOUBLE);
                            break;
                        }
                        preparedStatement.setDouble(i + 1, toDouble(objectList.get(i)));
                        break;
                    default:
                        throw new UnknownTypeException(type.name());
                }
                i++;
            }
        }
    }

    private long insert(Connection connection, PreparedStatement batchPreparedStatement, List<Object> objectList) throws SQLException {
        if (objectList.size() <= 0) {
            return 0;
        }
        if (objectList.size() == batchSize * columnTypeMap.size()) {
            setValues(batchPreparedStatement, objectList);
            return batchPreparedStatement.executeLargeUpdate();
        }
        PreparedStatement preparedStatement = prepareStatement(connection, objectList.size());
        setValues(preparedStatement, objectList);
        return preparedStatement.executeLargeUpdate();
    }

    private PreparedStatement prepareStatement(Connection connection, int size) {
        if (size < 1) {
            throw new IllegalArgumentException();
        }
        if (size % columnTypeMap.size() != 0) {
            throw new IllegalArgumentException(format("size: %d", size));
        }
        StringBuilder sb = new StringBuilder();
        sb.append(insertPrefix);
        for (int i = 0; i < size / columnTypeMap.size(); i++) {
            sb.append('(');
            for (int j = 0; j < columnTypeMap.size(); j++) {
                sb.append("?,");
            }
            sb.setLength(sb.length() - 1);
            sb.append("),");
        }
        sb.setLength(sb.length() - 1);
        try {
            return connection.prepareStatement(sb.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        for (int i = 0; i < thread; i++) {
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Connection connection = connect();
                    PreparedStatement batchPreparedStatement = prepareStatement(connection,
                            batchSize * columnTypeMap.size());
                    while (!Thread.interrupted()) {
                        try {
                            Table table = consume();
                            List<Column> columns = table.getColumns();
                            if (columns.size() != columnTypeMap.size()) {
                                throw new InconsistentColumnSizeException();
                            }

                            List<Object> values = new ArrayList<>();
                            for (int i = 0; i < table.size(); i++) {
                                for (int j = 0; j < columns.size(); j++) {
                                    values.add(columns.get(j).get(i));
                                }
                                if (values.size() == batchSize * columns.size()) {
                                    insert(connection, batchPreparedStatement, values);
                                    values.clear();
                                }
                            }

                            if (values.size() > 0) {
                                insert(connection, batchPreparedStatement, values);
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
