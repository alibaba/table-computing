package com.alibaba.tc.sp.output;

import com.alibaba.tc.offheap.ByteArray;
import com.alibaba.tc.sp.QueueSizeLogger;
import com.alibaba.tc.table.Column;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.Type;
import com.alibaba.sdb.jdbc.SdbDriver;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.tc.sp.StreamProcessing.handleException;
import static java.lang.Integer.parseInt;
import static java.lang.Integer.toHexString;
import static java.util.Objects.requireNonNull;

public class SdbOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(SdbOutputTable.class);
    private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");

    private final String address;
    private final String tableName;
    private final String indexes;
    private final int maxRetryTimes;
    private final int batchSize;
    private final boolean autoDropTable;
    protected final Map<String, Type> columnTypeMap;
    private final String sign;

    private final ThreadPoolExecutor threadPoolExecutor;
    private final QueueSizeLogger queueSizeLogger = new QueueSizeLogger();
    protected final QueueSizeLogger recordSizeLogger = new QueueSizeLogger();

    public SdbOutputTable(String address,
                          String tableName,
                          String indexes,
                          boolean autoDropTable,
                          Map<String, Type> columnTypeMap) throws IOException {
        this(Runtime.getRuntime().availableProcessors(),
                40000,
                address,
                tableName,
                indexes,
                1,
                autoDropTable,
                columnTypeMap);
    }

    public SdbOutputTable(int thread,
                          int batchSize,
                          String address,
                          String tableName,
                          String indexes,
                          int maxRetryTimes,
                          boolean autoDropTable,
                          Map<String, Type> columnTypeMap) throws IOException {
        super(thread);
        this.address = requireNonNull(address);
        this.tableName = requireNonNull(tableName);
        this.indexes = requireNonNull(indexes);
        this.maxRetryTimes = requireNonNull(maxRetryTimes);
        this.batchSize = batchSize;
        this.autoDropTable = autoDropTable;
        this.columnTypeMap = requireNonNull(columnTypeMap);
        this.sign = "|SdbOutputTable|" + tableName + "|" + toHexString(hashCode());

        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat("sdb-output-%d").build());

        createTable();
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:sdb://" + address + "/nsdb/default";
        Properties properties = new Properties();
        properties.setProperty("user", "user");
        return new SdbDriver().connect(url, properties);
    }

    private void createTable() throws IOException {
        StringBuilder fieldsBuilder = new StringBuilder();
        for (String columnName : columnTypeMap.keySet()) {
            Type type = columnTypeMap.get(columnName);
            JDBCType jdbcType = Type.toJDBCType(type);
            fieldsBuilder.append(columnName).append(" ").append(jdbcType).append(",");
        }

        String fieldsSchema = fieldsBuilder.toString();
        if (fieldsSchema.length() > 0) {
            fieldsSchema = fieldsSchema.substring(0, fieldsSchema.length() - 1);
        }

        /* 表不存在则创建表 */
        String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s (%s) ",
                tableName, fieldsSchema);
        if (!Strings.isNullOrEmpty(indexes)) {
            createTableSql += String.format(" WITH (indexes = '%s') ", indexes);
        }

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

        throw new IOException(">>> create sdb table error for " + tableName + ", we have tried " + maxRetryTimes + " times");
    }

    @Override
    public void produce(Table table) throws InterruptedException {
        queueSizeLogger.logQueueSize("Sdb输出队列大小" + sign, arrayBlockingQueueList);
        recordSizeLogger.logRecordSize("Sdb输出队列行数" + sign, arrayBlockingQueueList);
        putTable(table);
    }

    private int insert(OkHttpClient httpClient, List<Object> objectList) throws IOException {
        String resp = "";
        if (objectList.size() <= 0) {
            return 0;
        }
        StringBuilder sb = new StringBuilder();
        for (Object object : objectList) {
            if (object instanceof BigDecimal) {
                object = object.toString();
            }
            if (object instanceof String || object instanceof ByteArray) {
                sb.append('\'');
                sb.append(object.toString().replaceAll("'", "''"));
                sb.append('\'');
            } else {
                sb.append(object);
            }
            sb.append(",");
        }
        sb.setLength(sb.length() - 1);
        String query = sb.toString();

        String url = "http://" + address + "/v1/load/nsdb/default/" + tableName;
        Request request = new Request.Builder().url(url).post(RequestBody.create(MEDIA_TYPE_TEXT, query)).build();

        Response response = httpClient.newCall(request).execute();
        resp = response.body().string();
        int rows = parseInt(resp);

        return rows;
    }

    private OkHttpClient newHttpClient() {
        return new OkHttpClient.Builder().
                connectTimeout(60, TimeUnit.SECONDS).
                readTimeout(60, TimeUnit.SECONDS).
                writeTimeout(60, TimeUnit.SECONDS).
                build();
    }

    public void start() {
        for (int i = 0; i < thread; i++) {
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    OkHttpClient httpClient = newHttpClient();

                    while (!Thread.interrupted()) {
                        try {
                            Table table = consume();
                            List<Column> columns = table.getColumns();

                            List<Object> values = new ArrayList<>();
                            for (int i = 0; i < table.size(); i++) {
                                for (int j = 0; j < columns.size(); j++) {
                                    values.add(columns.get(j).get(i));
                                }
                                if (values.size() == batchSize * columns.size()) {
                                    insert(httpClient, values);
                                    values.clear();
                                }
                            }

                            if (values.size() > 0) {
                                insert(httpClient, values);
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
