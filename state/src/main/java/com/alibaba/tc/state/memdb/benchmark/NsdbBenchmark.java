/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.tc.state.memdb.benchmark;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static com.alibaba.nsdb.presto.DateWrapper.wrap;
import static com.alibaba.nsdb.presto.Threads.daemonThreadsNamed;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public final class NsdbBenchmark
{
    private static final Logger log = Logger.getLogger(NsdbBenchmark.class.getName());
    private long lastCount;
    private AtomicLong rowCount = new AtomicLong();
    private LinkedBlockingQueue<String> queueRows = new LinkedBlockingQueue<>();
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
            "      from SOURCE_BASIC_EVENT_SNMP_FAST s \n" +
            "      where (event_id = '1000005'\n" +
            "             or event_id = '1000004')\n" +
            "      and process_time_format >= '2020-01-14 00:08:00'\n" +
            "      and process_time_format < '2020-01-14 00:11:00'\n" +
            "      and circuit_group_name is not null\n" +
            "      and circuit_group_name != 'null'\n" +
            "      and circuit_group_name != ''\n" +
            "      and threshold = '150'\n" +
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
            "    having count(distinct hhmm) >= 2\n" +
            ") source\n" +
            "group by circuit_group_name\n";

    public static void logGreaterThan(String sql, long ts, long greaterThan)
    {
        long now = System.currentTimeMillis();
        if (now - ts > greaterThan) {
//            log.warning(format("sql: %s, time: %d", sql, (System.currentTimeMillis() - ts)/1000));
        }
    }

    private void queryWithConnection(String sql, Connection connection, List<JSONObject> rows)
    {
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            long ts = System.currentTimeMillis();
            resultSet = preparedStatement.executeQuery();
            logGreaterThan(sql, ts, 300);

            while (resultSet.next()) {
                JSONObject row = new JSONObject();
                //@param column the first column is 1, the second is 2, ... 见 getColumnLabel getObject 函数说明
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    row.put(resultSet.getMetaData().getColumnLabel(i), resultSet.getObject(i));
                }
                rows.add(row);
            }
            logGreaterThan("add fetch time: " + sql, ts, 300);
        }
        catch (SQLException e) {
            log.warning(Throwables.getStackTraceAsString(e));
        }
        finally {
            if (null != resultSet) {
                try {
                    resultSet.close();
                }
                catch (Throwable t) {
                    log.warning(Throwables.getStackTraceAsString(t));
                }
            }
        }
    }

    private Connection connect()
    {
        try {
            String url = "jdbc:presto://localhost:8080/nsdb/default";
            return DriverManager.getConnection(url, "user", null);
        }
        catch (Throwable t) {
            log.severe(Throwables.getStackTraceAsString(t));
        }

        return null;
    }

    private PreparedStatement prepareMysql(String sql) throws Throwable
    {
        String url = "jdbc:mysql://localhost:3306/perf";
        Connection connection = DriverManager.getConnection(url, "root", "");

        return connection.prepareStatement(sql);
    }

    private PreparedStatement prepare(String sql) throws Throwable
    {
        String url = "jdbc:presto://localhost:8081/nsdb/default";
//        String url = "jdbc:presto://localhost:8080/memory/default";
        Connection connection = DriverManager.getConnection(url, "user", null);

        return connection.prepareStatement(sql);
    }

    private long insert(PreparedStatement preparedStatement, List<String> params) throws Throwable
    {
        for (int i = 0; i < params.size(); i++) {
            preparedStatement.setString(i + 1, params.get(i));
        }
        return preparedStatement.executeLargeUpdate();
    }

    private long update(Connection connection, String sql, List<String> params) throws Throwable
    {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            preparedStatement.setString(i + 1, params.get(i));
        }
        return preparedStatement.executeUpdate();
    }

    private List<JSONObject> queryNowBackup(String sql) throws Throwable
    {
        String url = "jdbc:mysql://rm-now-backup.mysql.rds.tbsite.net:3306/now-backup";
        Connection connection = DriverManager.getConnection(url, "now", "huoshui");
        List<JSONObject> rows = new ArrayList<>(16);

        queryWithConnection(sql, connection, rows);

        return rows;
    }

    private List<JSONObject> queryMysql(String sql) throws Throwable
    {
        String url = "jdbc:mysql://localhost:3306/perf?useSSL=false";
        Connection connection = DriverManager.getConnection(url, "root", "");
        List<JSONObject> rows = new ArrayList<>(16);

        queryWithConnection(sql, connection, rows);

        return rows;
    }

    private List<JSONObject> query(String sql) throws Throwable
    {
        String url = "jdbc:presto://localhost:8081/nsdb/default";
//        String url = "jdbc:presto://localhost:8080/memory/default";
//        String url = "jdbc:presto://localhost:8080/mysql/perf";
        Connection connection = DriverManager.getConnection(url, "user", null);
        List<JSONObject> rows = new ArrayList<>(16);

        queryWithConnection(sql, connection, rows);

        return rows;
    }

    private List<JSONObject> queryPresto(String sql) throws Throwable
    {
//        String url = "jdbc:presto://localhost:8080/tddl/now_core_master";
        int port = 8080 + new Random().nextInt(6);
//        String url = "jdbc:presto://localhost:" + port + "/tddl/now_core_backup";
        String url = "jdbc:presto://localhost:" + port + "/tddl/now_core_master";
//        String url = "jdbc:presto://10.184.12.149:8080/tddl/now_core_backup";
        Connection connection = DriverManager.getConnection(url, "user", null);
        List<JSONObject> rows = new ArrayList<>(16);

        queryWithConnection(sql, connection, rows);

        return rows;
    }

    private Runnable queryRunnable()
    {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        query(sqlNoJoin);
                        rowCount.incrementAndGet();
                        Thread.sleep(1000);
                    } catch (Throwable t) {
                        log.severe(Throwables.getStackTraceAsString(t));
                    }
                }
            }
        };
    }

    private Runnable queryRunnableInSelect()
    {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        query("select circuit_group_name,\n" +
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
                                "      COALESCE((select stat_num from dim__event_obj_stat where obj_name = circuit_group_name and stat_type = 'LINK_NUM_OF_NEW_CIRCUIT_GROUP'), '0')  as link_num\n" +
                                "      from SOURCE_BASIC_EVENT_SNMP_FAST s \n" +
                                "      where (event_id = '1000005'\n" +
                                "             or event_id = '1000004')\n" +
                                "      and process_time_format >= '2020-01-14 00:08:00'\n" +
                                "      and process_time_format < '2020-01-14 00:11:00'\n" +
                                "      and circuit_group_name is not null\n" +
                                "      and circuit_group_name != 'null'\n" +
                                "      and circuit_group_name != ''\n" +
                                "      and threshold = '150'\n" +
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
                                "    having count(distinct hhmm) >= 2\n" +
                                ") source\n" +
                                "group by circuit_group_name\n");
                        rowCount.incrementAndGet();
                    } catch (Throwable t) {
                        log.severe(Throwables.getStackTraceAsString(t));
                    }
                }
            }
        };
    }

    private Runnable queryRunnableJoin(boolean swi)
    {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        query("select circuit_group_name,\n" +
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
                                "      COALESCE(d.stat_num, '0') as link_num\n"
                                +

                                (!swi ? "      from SOURCE_BASIC_EVENT_SNMP_FAST s left join dim__event_obj_stat d\n" : "      from dim__event_obj_stat d left join SOURCE_BASIC_EVENT_SNMP_FAST s \n" )

                                +
                                "        on s.circuit_group_name = d.obj_name and d.stat_type = 'LINK_NUM_OF_NEW_CIRCUIT_GROUP'\n" +
                                "      where (event_id = '1000005'\n" +
                                "             or event_id = '1000004')\n" +
                                "      and process_time_format >= '2020-01-14 00:08:00'\n" +
                                "      and process_time_format < '2020-01-14 00:11:00'\n" +
                                "      and circuit_group_name is not null\n" +
                                "      and circuit_group_name != 'null'\n" +
                                "      and circuit_group_name != ''\n" +
                                "      and threshold = '150'\n" +
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
                                "    having count(distinct hhmm) >= 2\n" +
                                ") source\n" +
                                "group by circuit_group_name\n");
                        rowCount.incrementAndGet();
                    } catch (Throwable t) {
                        log.severe(Throwables.getStackTraceAsString(t));
                    }
                }
            }
        };
    }

    private Runnable queryRunnableJoinMysql()
    {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        query("select circuit_group_name,\n" +
                                "-- concat(circuit_group_name, ' |电路组错包过高| ', cast(sum(peer_times) as varchar), '/', cast(max(cast(link_num as int)) as varchar)) as brief,\n" +
                                "group_concat(tracking_ids) as tracking_ids,\n" +
                                "group_concat(DISTINCT scan_objs) as scan_objs,\n" +
                                "group_concat(distinct host_name) as host_name\n" +
                                "from (\n" +
                                "  select port_key,circuit_group_name,count(distinct hhmm) as times,\n" +
                                "  count(distinct peer_name) as peer_times,\n" +
                                "  max(link_num) as link_num,\n" +
                                "  group_concat(distinct host_name) as host_name,\n" +
                                "  group_concat(message_id) as tracking_ids,\n" +
                                "  group_concat(distinct event_obj) as scan_objs\n" +
                                "  from (\n" +
                                "      select concat(host_name, '#', port_name) as event_obj,host_name,\n" +
                                "      circuit_group_name, message_id,\n" +
                                "      port_key,port_key as peer_name,\n" +
                                "      log_time_HHmm_format as hhmm,\n" +
                                "      COALESCE(d.stat_num, 0) as link_num\n" +
                                "      from SOURCE_BASIC_EVENT_SNMP_FAST s force index(idx_process_time_format_event_id) left join dim__event_obj_stat d\n" +
                                "        on s.circuit_group_name = d.obj_name and d.stat_type = 'LINK_NUM_OF_NEW_CIRCUIT_GROUP'\n" +
                                "      where (event_id = 1000005\n" +
                                "             or event_id = 1000004)\n" +
                                "      and process_time_format >= '2020-01-14 00:08:00'\n" +
                                "      and process_time_format < '2020-01-14 00:11:00'\n" +
                                "      and circuit_group_name is not null\n" +
                                "      and circuit_group_name != 'null'\n" +
                                "      and circuit_group_name != ''\n" +
                                "      and threshold = '150'\n" +
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
                                "    having count(distinct hhmm) >= 2\n" +
                                ") source\n" +
                                "group by circuit_group_name\n");
                        rowCount.incrementAndGet();
                    } catch (Throwable t) {
                        log.severe(Throwables.getStackTraceAsString(t));
                    }
                }
            }
        };
    }

    private Runnable fixedRunable()
    {
        return new Runnable() {
            @Override
            public void run()
            {
//                final String insertTable = "insert into dim__event_obj_stat values ";
                final String insertTable = "insert into source_basic_event_snmp_fast values ";
                String sql = insertTable;
                List<String> insertParams = new ArrayList<>();
                int updateCount = 0;
                PreparedStatement preparedStatement = null;
                while (true) {
                    try {
                        String row = queueRows.poll(1, TimeUnit.SECONDS);
                        if (null == row) {
                            //超时没取到
                            if (!insertParams.isEmpty()) {
                                sql = sql.substring(0, sql.length() - 2);
                                preparedStatement = prepare(sql);
                                insert(preparedStatement, insertParams);
                                insertParams.clear();
                                sql = insertTable;
                            }
                            continue;
                        }
                        rowCount.incrementAndGet();
                        try {
                            String[] fields = row.split("\",\"");
                            String qMarks = String.join(",", Collections.nCopies(fields.length, "?"));
                            sql += " (" + qMarks + "), ";
                            insertParams.addAll(Arrays.asList(fields));
                            updateCount++;
                            if (updateCount % 500 == 0) {
                                if (preparedStatement == null) {
                                    sql = sql.substring(0, sql.length() - 2);
                                    preparedStatement = prepare(sql);
                                }
                                insert(preparedStatement, insertParams);

                                insertParams.clear();
                                sql = insertTable;
                            }
                        }
                        catch (Throwable t) {
                            insertParams.clear();
                            sql = insertTable;
                            log.severe(Throwables.getStackTraceAsString(t));
                        }
                    }
                    catch (Throwable t) {
                        log.severe(Throwables.getStackTraceAsString(t));
                    }
                }
            }
        };
    }

    private Runnable varRunable(int kB)
    {
        return new Runnable() {
            @Override
            public void run()
            {
                String sql = "insert into source_basic_event_snmp_fast values ";
                List<String> insertParams = new ArrayList<>();
                while (true) {
                    try {
                        String row = queueRows.take();
                        rowCount.incrementAndGet();
                        try {
                            String[] fields = row.split("\",\"");
                            String qMarks = String.join(",", Collections.nCopies(fields.length, "?"));
                            if (sql.length() + qMarks.length() + 2 >= kB * 1024) {
                                sql = sql.substring(0, sql.length() - 2);

                                Connection connection = connect();
                                update(connection, sql, insertParams);
                                connection.close();

                                insertParams.clear();
                                sql = "insert into source_basic_event_snmp_fast values ";
                            }
                            sql += " (" + qMarks + "), ";
                            insertParams.addAll(Arrays.asList(fields));
                        }
                        catch (Throwable t) {
                            insertParams.clear();
                            sql = "insert into source_basic_event_snmp_fast values ";
                            log.severe(Throwables.getStackTraceAsString(t));
                        }
                    }
                    catch (Throwable t) {
                        log.severe(Throwables.getStackTraceAsString(t));
                    }
                }
            }
        };
    }

    private Runnable queryRunnableTddl(List<String> codes)
    {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        for (String code : codes) {
                            String sql = wrap(code, System.currentTimeMillis());
                            queryPresto(sql);
//                            queryPresto(sqlNoJoin);
                            rowCount.incrementAndGet();
                        }
                    } catch (Throwable t) {
                        log.severe(Throwables.getStackTraceAsString(t));
                    }
                }
            }
        };
    }

    private void bench(int nThreads, int kB)
    {
        long start = System.currentTimeMillis();

        newSingleThreadScheduledExecutor(daemonThreadsNamed("nsdb-benchmark-%s")).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run()
            {
                try {
                    String info = format("total: %d, tps: %f, last tps: %d, queue size: %d", rowCount.longValue(), rowCount.longValue() / ((System.currentTimeMillis() - start) / 1_000.), rowCount.longValue() - lastCount, queueRows.size());
                    log.info(info);
                    lastCount = rowCount.longValue();
                }
                catch (Throwable t) {
                    log.severe(Throwables.getStackTraceAsString(t));
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

//        Runnable runnable = fixedRunable();
//        Runnable runnable = varRunable(kB);
//        Runnable runnable = queryRunnable();
        Runnable runnable = queryRunnableJoin(false);
//        Runnable runnable = queryRunnableJoinMysql();

//        List<String> codes = new ArrayList<>();
//        try {
//            DriverManager.registerDriver(new com.facebook.presto.jdbc.PrestoDriver());
//            List<JSONObject> list = queryNowBackup("SELECT code FROM now__job_info j, now__job_pub jp, now__code_log c WHERE j.id = jp.job_id and jp.code_id = c.id and jp.tag = 'ONLINE' AND code LIKE '%source_basic_event_snmp_fast%' and j.online_setting_status = 'STARTED'");
//            for (JSONObject jsonObject : list) {
//                codes.add(jsonObject.getString("code"));
//            }
//        } catch (Throwable t) {
//            log.severe(Throwables.getStackTraceAsString(t));
//        }
//
//        Runnable runnable = queryRunnableTddl(codes);

        ExecutorService executorService = newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            executorService.submit(runnable);
        }

//        try {
//            Files.lines(Paths.get("/tmp/snmp_fast.data")).forEach(row -> {
////            Files.lines(Paths.get("/tmp/snmp_fast_108.data")).forEach(row -> {
////            Files.lines(Paths.get("/tmp/dim__event_obj_stat.data")).forEach(row -> {
//                queueRows.offer(row);
//            });
//        }
//        catch (Throwable t) {
//            log.severe(Throwables.getStackTraceAsString(t));
//        }
    }

    public static void main(String[] args)
    {
        new NsdbBenchmark().bench(parseInt(args[0]), parseInt(args[1]));
    }
}
