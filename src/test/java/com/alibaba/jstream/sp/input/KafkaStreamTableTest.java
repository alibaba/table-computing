package com.alibaba.jstream.sp.input;

import com.alibaba.jstream.sp.output.KafkaOutputTable;
import com.alibaba.jstream.table.ColumnTypeBuilder;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.TableBuilder;
import com.alibaba.jstream.table.Type;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.*;

public class KafkaStreamTableTest {
    /**
     * kafka_2.13-2.8.0/bin/zookeeper-server-start.sh config/zookeeper.properties
     * kafka_2.13-2.8.0/bin/kafka-server-start.sh config/server.properties
     * kafka_2.13-2.8.0/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic my_topic_name --partitions 1
     */

    @Test
    public void produceThenConsume() throws InterruptedException {
        Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
                .column("testColumnVarchar", Type.VARCHAR)
                .column("testColumnInt", Type.INT)
                .column("testColumnBigint", Type.BIGINT)
                .column("testColumnDouble", Type.DOUBLE)
                .build();
        String bootstrapServers = "localhost:9092";
        String topic = "testTopic";
        KafkaOutputTable kafkaOutputTable = new KafkaOutputTable(bootstrapServers, topic);
        kafkaOutputTable.start();
        Table table = new TableBuilder(columnTypeMap)
                .append(0, "c1v1")
                .append(1, 1)
                .append(2, Long.MAX_VALUE)
                .append(3, Double.MAX_VALUE)
                .appendValue(0, null)
                .append(1, 2)
                .append(2, Long.MIN_VALUE)
                .append(3, Double.MIN_VALUE)
                .build();
        kafkaOutputTable.produce(table);

        columnTypeMap = new ColumnTypeBuilder()
                .column("testColumnVarchar", Type.VARCHAR)
                .column("testColumnInt", Type.INT)
                .column("testColumnBigint", Type.BIGINT)
                .column("__time__", Type.BIGINT)
                .column("testColumnDouble", Type.DOUBLE)
                .build();
        KafkaStreamTable kafkaStreamTable = new KafkaStreamTable(bootstrapServers,
                "consumerGroupId",
                topic,
                new Calendar.Builder().
                        setDate(2021, 9, 17).
                        setTimeOfDay(11, 3, 0).
                        build().
                        getTimeInMillis(),
                new Calendar.Builder().
                        setDate(2021, 9, 17).
                        setTimeOfDay(11, 3, 0).
                        build().
                        getTimeInMillis(),
                columnTypeMap);
        kafkaStreamTable.start();
        table = kafkaStreamTable.consume();
        Thread.sleep(1_000);
        table = kafkaStreamTable.consume();

        assert table.getColumn(0).getString(0).equals("c1v1");
        assert table.getColumn(1).getInteger(0).equals(1);
        assert table.getColumn(2).getLong(0).equals(Long.MAX_VALUE);
        assert table.getColumn(0).getString(1) == null;
        assert table.getColumn(4).getDouble(1) == Double.MIN_VALUE;
    }
}