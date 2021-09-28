package com.alibaba.tc.sp.input;

import com.alibaba.tc.function.AggTimeWindowFunction;
import com.alibaba.tc.function.ScalarFunction;
import com.alibaba.tc.function.TimeWindowFunction;
import com.alibaba.tc.sp.Compute;
import com.alibaba.tc.sp.Rehash;
import com.alibaba.tc.sp.StreamProcessing;
import com.alibaba.tc.sp.dimension.MysqlDimensionTable;
import com.alibaba.tc.sp.dimension.TableIndex;
import com.alibaba.tc.sp.output.KafkaOutputTable;
import com.alibaba.tc.table.ColumnTypeBuilder;
import com.alibaba.tc.table.Row;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.TableBuilder;
import com.alibaba.tc.table.Type;
import com.alibaba.tc.util.AggregationUtil;
import com.alibaba.tc.util.WindowUtil;
import com.alibaba.tc.window.SessionWindow;
import com.alibaba.tc.window.SlideWindow;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Top100Test {
    /**
     * cd kafka_2.13-2.8.0
     * bin/zookeeper-server-start.sh config/zookeeper.properties
     * bin/kafka-server-start.sh config/server.properties
     * bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic my_topic_name --partitions 1
     */

    private void produce(String bootstrapServers, String topic, Map<String, Type> columnTypeMap) throws InterruptedException {
        KafkaOutputTable kafkaOutputTable = new KafkaOutputTable(1, bootstrapServers, topic);
        kafkaOutputTable.start();
        Table table = new TableBuilder(columnTypeMap)
                .append(0, 0L)
                .append(1, 1L)
                .append(2, 1)
                .append(3, 3)

                .append(0, 3000L)
                .append(1, 2L)
                .append(2, 3)
                .append(3, 2)

                .append(0, 50000L)
                .append(1, 3L)
                .append(2, 2)
                .append(3, 1)

                .append(0, 3599000L)
                .append(1, 4L)
                .append(2, 3)
                .append(3, 1)

                .append(0, 3600000L)
                .append(1, 5L)
                .append(2, 2)
                .append(3, 1)

                .append(0, 5399000L)
                .append(1, 6L)
                .append(2, 1)
                .append(3, 5)

                .build();
        kafkaOutputTable.produce(table);
    }

    @Test
    public void top100() throws InterruptedException {
        MysqlDimensionTable mysqlDimensionTable = new MysqlDimensionTable("jdbc:mysql://localhost:3306/e-commerce",
                "commodity",
                "userName",
                "password",
                Duration.ofHours(1),
                new ColumnTypeBuilder()
                        .column("id", Type.INT)
                        .column("name", Type.VARCHAR)
                        .column("price", Type.INT)
                        .build(),
                "id"
        );

        Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
                .column("__time__", Type.BIGINT)
                .column("id", Type.BIGINT)
                .column("commodity_id", Type.INT)
                .column("count", Type.INT)
                .build();
        String bootstrapServers = "localhost:9092";
        String topic = "order";
        produce(bootstrapServers, topic, columnTypeMap);

        KafkaStreamTable kafkaStreamTable = new KafkaStreamTable(bootstrapServers,
                "consumerGroupId",
                topic,
                0,
                columnTypeMap);
        kafkaStreamTable.start();

        StreamProcessing sp = new StreamProcessing(1);
        String[] hashBy = new String[]{"commodity_id"};
        Rehash rehashForSlideWindow = sp.rehash("uniqueNameForSlideWindow", hashBy);
        String[] returnedColumns = new String[]{"commodity_id",
                "sales_volume",
                "saleroom",
                "window_start"};
        SlideWindow slideWindow = new SlideWindow(Duration.ofHours(1),
                Duration.ofMinutes(30),
                hashBy,
                "__time__",
                new AggTimeWindowFunction() {
                    @Override
                    public Comparable[] agg(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                        return new Comparable[]{
                                partitionByColumns.get(0),
                                AggregationUtil.sumInt(rows, "count"),
                                AggregationUtil.sumInt(rows, "total_price"),
                                windowStart
                        };
                    }
                }, returnedColumns);
        slideWindow.setWatermark(Duration.ofSeconds(2));

        hashBy = new String[]{"window_start"};
        Rehash rehashForSessionWindow = sp.rehash("uniqueNameForSessionWindow", hashBy);
        SessionWindow sessionWindow = new SessionWindow(Duration.ofSeconds(1),
                hashBy,
                "window_start",
                new TimeWindowFunction() {
                    @Override
                    public List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                        int[] top100 = WindowUtil.topN(rows, "sales_volume", 100);
                        List<Comparable[]> ret = new ArrayList<>(100);
                        for (int i = 0; i < top100.length; i++) {
                            ret.add(rows.get(top100[i]).getAll());
                        }
                        return ret;
                    }
                }, returnedColumns);
        sessionWindow.setWatermark(Duration.ofSeconds(3));

        sp.compute(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                Table table = kafkaStreamTable.consume();
                table = table.select(new ScalarFunction() {
                    @Override
                    public Comparable[] returnOneRow(Row row) {
                        TableIndex tableIndex = mysqlDimensionTable.curTable();
                        // Use tableIndex.getRow but not mysqlDimensionTable.curTable().getRow. Consider that in some case
                        // you may need to call mysqlDimensionTable.curTable() twice but the second call may correspond
                        // to the newly reloaded dimension table which is not consistent with the first mysqlDimensionTable.curTable()
                        Row commodity = tableIndex.getRow(row.getInteger("commodity_id"));
                        return new Comparable[]{
                                commodity.getString("name"),
                                commodity.getInteger("price"),
                                row.getInteger("count") * commodity.getInteger("price")
                        };
                    }
                }, true, "commodity_name", "commodity_price", "total_price");
                List<Table> tables = rehashForSlideWindow.rehash(table, myThreadIndex);
                table = slideWindow.slide(tables);
                tables = rehashForSessionWindow.rehash(table, myThreadIndex);
                table = sessionWindow.session(tables);
                if (table.size() > 0) {
                    table.print();
                    assert table.size() == 3;
                    assert table.getColumn("commodity_id").getInteger(0) == 3;
                    assert table.getColumn("commodity_id").getInteger(2) == 2;
                    assert table.getColumn("window_start").getLong(0) == 0L;
                    //elegantly finish the streaming task when terminate condition is satisfied
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
}