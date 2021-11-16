package com.alibaba.tc.state.memdb.benchmark;

import com.alibaba.sdb.jdbc.SdbDriver;
import com.facebook.airlift.log.Logger;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class Benchmark {
    private static final Logger log = Logger.get(Benchmark.class);
    private long lastCount;
    private AtomicLong rowCount = new AtomicLong();
    private AtomicInteger fieldsCount = new AtomicInteger();
    private LinkedBlockingQueue<String> queueRows = new LinkedBlockingQueue<>(1000000);
    private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");
    private static final OkHttpClient httpClient = new OkHttpClient.Builder().build();
    private String server = "127.0.0.1:8080";

    private final String dimTable = "create table if not exists dim__event_obj_stat (\n" +
            "\tobj_name varchar,\n" +
            "\tstat_type varchar,\n" +
            "\tstat_num int\n" +
            ") with (allow_directly_rename_to = 'true')";
    private final String createTable = "CREATE TABLE IF NOT EXISTS snmp_bench (\n" +
            "   id0 varchar(65535),\n" +
            "   gmt_create varchar(65535),\n" +
            "   gmt_modified varchar(65535),\n" +
            "   host_name varchar(65535),\n" +
            "   port_name varchar(65535),\n" +
            "   event_id bigint,\n" +
            "   event_name varchar(65535),\n" +
            "   logic_site_name varchar(65535),\n" +
            "   dsw_cluster_name varchar(65535),\n" +
            "   logic_pod_name varchar(65535),\n" +
            "   role boolean,\n" +
            "   threshold smallint,\n" +
            "   actual_value double,\n" +
            "   brand_name varchar(65535),\n" +
            "   network_model_name varchar(65535),\n" +
            "   op_host_name varchar(65535),\n" +
            "   op_port_name varchar(65535),\n" +
            "   port_key varchar(65535),\n" +
            "   circuit_group_type varchar(65535),\n" +
            "   circuit_type varchar(65535),\n" +
            "   circuit_group_name varchar(65535),\n" +
            "   circuit_name varchar(65535),\n" +
            "   message_id varchar(65535),\n" +
            "   log_time varchar(65535),\n" +
            "   log_time_format varchar(65535),\n" +
            "   log_time_hhmm_format varchar(65535),\n" +
            "   process_time varchar(65535),\n" +
            "   process_time_format varchar(256),\n" +
            "   process_time_hhmm_format varchar(65535),\n" +
            "   real_port_name varchar(65535),\n" +
            "   compare_value varchar,\n" +
            "   bandwidth decimal(32, 6),\n" +
            "   app_server_state varchar(65535),\n" +
            "   uuid varchar(64),\n" +
            "   local_ip varchar(134),\n" +
            "   remote_ip varchar(134)\n" +
            ")\n" +
            "WITH (\n" +
//            "\tindexes = '[{\"index1\": [\"event_id\", \"threshold\", \"process_time_format\", \"actual_value\", \"role\"]}, {\"index1\": [\"threshold\", \"process_time_format\"]}]'\n" +
            "\tindexes = '[{\"index1\": [\"process_time_format\", \"threshold\", \"event_id\"]}]'\n" +
//            "\tindexes = '[{\"index1\": [\"process_time_format\"]}]'\n" +
            ")\n";

    private final String sqlNoJoin = "select circuit_group_name,\n" +
            "concat(circuit_group_name, ' |电路组错包过高| ', cast(sum(peer_times) as varchar), '/',cast(max(link_num) as varchar)) as brief,\n" +
            "array_join(array_agg(tracking_ids), ',') as tracking_ids,\n" +
            "array_join(array_agg(DISTINCT scan_objs), ',') as scan_objs,\n" +
            "array_join(array_agg(distinct host_name), ',') as host_name\n" +
            "from (\n" +
            "  select port_key,circuit_group_name,count(distinct hhmm) as times,\n" +
            "  count(distinct peer_name) as peer_times,\n" +
            "  max(link_num) as link_num,\n" +
            "  array_join(array_agg(distinct host_name), ',') as host_name,\n" +
            "  array_join(array_agg(message_id), ',') as tracking_ids,\n" +
            "  array_join(array_agg(distinct event_obj), ',') as scan_objs\n" +
            "  from (\n" +
            "      select concat(host_name, '#', port_name) as event_obj,host_name,\n" +
            "      circuit_group_name, message_id,\n" +
            "      port_key,port_key as peer_name,\n" +
            "      log_time_HHmm_format as hhmm,\n" +
            "      '0' as link_num\n" +
            "      from snmp_bench s left join dim__event_obj_stat d \n" +
            "        on s.circuit_group_name = d.obj_name and d.stat_type = 'LINK_NUM_OF_NEW_CIRCUIT_GROUP'" +
            "      where (event_id = 1000005\n" +
            "             or event_id = 1000004)\n" +
            "      and process_time_format >= '2019-12-08 09:07:00'\n" +
            "      and process_time_format < '2019-12-09 20:40:30'\n" +
            "      and bandwidth >= 0\n" +
            "      and circuit_group_name is not null\n" +
            "      and circuit_group_name != 'null'\n" +
            "      and circuit_group_name != ''\n" +
            "      and threshold = 150\n" +
            "      and CIRCUIT_GROUP_TYPE != '网关电路'\n" +
            "      and CIRCUIT_GROUP_TYPE != '外联专线'\n" +
            "      and circuit_group_type != 'FEX'\n" +
            "      and host_name != ''\n" +
            "      AND host_name is not null\n" +
            "      AND port_name != ''\n" +
            "      AND port_name is not null\n" +
            "      and logic_site_name not like '%CDN%'\n" +
            "    ) source\n" +
            "    group by circuit_group_name,port_key\n" +
            "    having count(distinct hhmm) >= 1\n" +
            ") source\n" +
            "group by circuit_group_name\n";

    private Connection connect() throws Throwable
    {
        String url = "jdbc:sdb://" + server + "/nsdb/default";
        Properties properties = new Properties();
        properties.setProperty("user", "user");
        return new SdbDriver().connect(url, properties);
    }

    private PreparedStatement prepare(Connection connection, String sql) throws Throwable
    {
        return connection.prepareStatement(sql);
    }

    private List<Map<String, Object>> executeQuery(PreparedStatement preparedStatement, List<String> params) throws Throwable
    {
        for (int i = 0; i < params.size(); i++) {
            preparedStatement.setString(i + 1, params.get(i));
        }

        ResultSet resultSet = preparedStatement.executeQuery();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> row = new HashMap<>();
            //@param column the first column is 1, the second is 2, ... 见 getColumnLabel getObject 函数说明
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                row.put(resultSet.getMetaData().getColumnLabel(i), resultSet.getObject(i));
            }
            rows.add(row);
        }

        return rows;
    }

    private long executeUpdate(PreparedStatement preparedStatement, List<String> params) throws Throwable
    {
        for (int i = 0; i < params.size(); i++) {
            if (i % fieldsCount.get() == 5) {
                preparedStatement.setLong(i + 1, Long.parseLong(params.get(i)));
            }
            else if (i % fieldsCount.get() == 10) {
                preparedStatement.setBoolean(i + 1, params.get(i).equalsIgnoreCase("ASW"));
            }
            else if (i % fieldsCount.get() == 11) {
                preparedStatement.setInt(i + 1, parseInt(params.get(i)));
            }
            else if (i % fieldsCount.get() == 12) {
                preparedStatement.setDouble(i + 1, parseDouble(params.get(i)));
            }
            else if (i % fieldsCount.get() == 31) {
                preparedStatement.setBigDecimal(i + 1, new BigDecimal(params.get(i)));
            }
            else {
                preparedStatement.setString(i + 1, params.get(i));
            }
        }
        return preparedStatement.executeLargeUpdate();
    }

    private Runnable fixedRunable()
    {
        return new Runnable() {
            @Override
            public void run()
            {
//                final String insertTable = "insert into dim__event_obj_stat values ";
                final String insertTable = "insert into snmp_bench values ";
                String sql = insertTable;
                List<String> insertParams = new ArrayList<>();
                int updateCount = 0;

                Connection connection = null;
                try {
                    connection = connect();
                }
                catch (Throwable t) {
                    log.error(t);
                }

                PreparedStatement preparedStatement = null;
                while (true) {
                    try {
                        String row = queueRows.poll(1, TimeUnit.SECONDS);
                        if (null == row) {
                            //超时没取到
                            if (!insertParams.isEmpty()) {
                                sql = sql.substring(0, sql.length() - 2);
                                preparedStatement = prepare(connection, sql);
                                executeUpdate(preparedStatement, insertParams);
                                insertParams.clear();
                                sql = insertTable;
                            }
                            continue;
                        }
                        rowCount.incrementAndGet();
                        try {
                            String[] fields = row.split("\",\"");
                            fieldsCount.set(fields.length);
                            String qMarks = String.join(",", Collections.nCopies(fieldsCount.get(), "?"));
                            sql += " (" + qMarks + "), ";
                            insertParams.addAll(Arrays.asList(fields));
                            updateCount++;
                            if (updateCount % 500 == 0) {
                                if (preparedStatement == null) {
                                    sql = sql.substring(0, sql.length() - 2);
                                    preparedStatement = prepare(connection, sql);
                                }
                                executeUpdate(preparedStatement, insertParams);

                                insertParams.clear();
                                sql = insertTable;
                            }
                        }
                        catch (Throwable t) {
                            insertParams.clear();
                            sql = insertTable;
                            log.error(t);
                        }
                    }
                    catch (Throwable t) {
                        log.error(t);
                    }
                }
            }
        };
    }

    private long executeUpdate(List<String> params)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.size(); i++) {
            if (i % fieldsCount.get() == 5) {
                sb.append(params.get(i));
            }
            else if (i % fieldsCount.get() == 10) {
                Boolean bool = params.get(i).equalsIgnoreCase("ASW");
                sb.append(bool.toString());
            }
            else if (i % fieldsCount.get() == 11) {
                sb.append(params.get(i));
            }
            else if (i % fieldsCount.get() == 12) {
                sb.append(params.get(i));
            }
            else if (i % fieldsCount.get() == 31) {
                sb.append(params.get(i));
            }
            else {
                sb.append('\'');
                sb.append(params.get(i).replaceAll("'", "''"));
                sb.append('\'');
            }
            if (i < params.size() - 1) {
                sb.append(',');
            }
        }
        String query = sb.toString();
        Request request = new Request.Builder().url("http://" + server + "/v1/load/nsdb/default/snmp_bench").post(RequestBody.create(query, MEDIA_TYPE_TEXT)).build();
        try {
            Response response = httpClient.newCall(request).execute();
            try {
                return parseInt(response.body().string());
            }
            catch (NumberFormatException e) {
                log.error(e, response.message());
            }
        } catch (IOException e) {
            log.error(e);
        }

        return -1;
    }

    private Runnable loadRunable()
    {
        return new Runnable() {
            @Override
            public void run()
            {
                List<String> insertParams = new ArrayList<>();
                int updateCount = 0;

                while (true) {
                    try {
                        String row = queueRows.poll(1, TimeUnit.SECONDS);
                        if (null == row) {
                            //超时没取到
                            if (!insertParams.isEmpty()) {
                                executeUpdate(insertParams);
                                insertParams.clear();
                            }
                            continue;
                        }
                        rowCount.incrementAndGet();
                        try {
                            String[] fields = row.split("\",\"");
                            fieldsCount.set(fields.length);
                            insertParams.addAll(Arrays.asList(fields));
                            updateCount++;
                            if (updateCount % 500 == 0) {
                                executeUpdate(insertParams);
                                insertParams.clear();
                            }
                        }
                        catch (Throwable t) {
                            insertParams.clear();
                            log.error(t);
                        }
                    }
                    catch (Throwable t) {
                        log.error(t);
                    }
                }
            }
        };
    }

    private void startStats()
    {
        long start = System.currentTimeMillis();

        newSingleThreadScheduledExecutor(daemonThreadsNamed("nsdb-benchmark-%s")).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run()
            {
                try {
                    String info = format("total: %d, tps: %f, last tps: %f, queue size: %d, conns: %d",
                            rowCount.longValue(),
                            rowCount.longValue() / ((System.currentTimeMillis() - start) / 1_000.),
                            (rowCount.longValue() - lastCount) / 5.,
                            queueRows.size(),
                            httpClient.connectionPool().connectionCount());
                    log.info(info);
                    lastCount = rowCount.longValue();
                }
                catch (Throwable t) {
                    log.error(t);
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void benchInsert(int nThreads, boolean isLoop, Runnable runnable)
    {
        ExecutorService executorService = newFixedThreadPool(nThreads + 1);
        for (int i = 0; i < nThreads; i++) {
            executorService.submit(runnable);
        }

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    do {
                        Files.lines(Paths.get("/tmp/snmp_fast.data")).forEach(row -> {
//                        Files.lines(Paths.get("/Users/shanqiang/Documents/snmp_fast_108.data")).forEach(row -> {
                            try {
                                queueRows.put(row);
                            } catch (InterruptedException e) {
                                log.error(e);
                            }
                        });
                    } while (isLoop);
                }
                catch (Throwable t) {
                    log.error(t);
                }
            }
        });
    }

    private void benchSelect(int nThreads)
    {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    Connection connection = connect();
                    PreparedStatement preparedStatement = prepare(connection, sqlNoJoin);
                    while (true) {
                        try {
                            List<Map<String, Object>> rows = executeQuery(preparedStatement, new ArrayList<>());
                            rowCount.incrementAndGet();
                        }
                        catch (Throwable t) {
                            log.error(t);
                        }
                    }
                }
                catch (Throwable t) {
                    log.error(t);
                }
            }
        };

        ExecutorService executorService = newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            executorService.submit(runnable);
        }
    }

    private void init()
    {
        try {
            Connection connection = connect();
            prepare(connection, createTable).execute();
            prepare(connection, dimTable).execute();
        }
        catch (Throwable t) {
            log.error(t);
        }
    }

    public static void main(String[] args)
    {
        int threads = parseInt(args[1]);
        Benchmark benchmark = new Benchmark();
        if (args.length > 3) {
            benchmark.server = args[3];
        }
        benchmark.init();
        benchmark.startStats();
        if ("insert".equalsIgnoreCase(args[0])) {
            benchmark.benchInsert(threads, args.length > 2 && args[2] != null && "loop".equalsIgnoreCase(args[2]), benchmark.fixedRunable());
        }
        else if ("select".equalsIgnoreCase(args[0])) {
            benchmark.benchSelect(threads);
        }
        else if ("load".equalsIgnoreCase(args[0])) {
            benchmark.benchInsert(threads, args.length > 2 && args[2] != null && "loop".equalsIgnoreCase(args[2]), benchmark.loadRunable());
        }
        else {
            log.error("The first param is neither insert nor select!");
        }
    }
}
