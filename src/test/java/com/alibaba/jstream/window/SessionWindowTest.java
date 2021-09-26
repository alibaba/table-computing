package com.alibaba.jstream.window;

import com.alibaba.jstream.function.TimeWindowFunction;
import com.alibaba.jstream.offheap.ByteArray;
import com.alibaba.jstream.sp.Compute;
import com.alibaba.jstream.sp.Rehash;
import com.alibaba.jstream.sp.StreamProcessing;
import com.alibaba.jstream.sp.input.InsertableStreamTable;
import com.alibaba.jstream.table.ColumnTypeBuilder;
import com.alibaba.jstream.table.Row;
import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.Type;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionWindowTest {

    @Test
    public void session() {
        Map<String, Type> columnTypeMap = new ColumnTypeBuilder().
                column("firstPartitionByColumn", Type.VARCHAR).
                column("secondPartitionByColumn", Type.VARCHAR).
                column("ts", Type.BIGINT).
                build();
        InsertableStreamTable insertableStreamTable = new InsertableStreamTable(1, columnTypeMap);
        insertableStreamTable.sleepMsWhenNoData(2000);
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 3L);

        //下面两个表time字段值都是10，watermark之后会成为2行记录的一个表
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 10L);
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 10L);

        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 13L);

        //下面这条记录触发了前面4条记录的计算（windowTimeout=10ms）
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 23L);

        //下面这条记录触发了上面time=23的那条记录的计算（windowTimeout=10ms）
        //这条记录之后没有数据了所以要等到noDataDelay（2000ms）之后才会触发这条记录的计算
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 103L);

        Map<List<Comparable>, Map<List<Long>, Integer>> mapMap = new HashMap<>();
        mapMap.put(new ArrayList<Comparable>(2){{
            add(new ByteArray("firstPartitionByColumn1"));
            add(new ByteArray("secondPartitionByColumn1"));
        }}, new HashMap<>());
        mapMap.put(new ArrayList<Comparable>(2){{
            add(new ByteArray("firstPartitionByColumn2"));
            add(new ByteArray("secondPartitionByColumn2"));
        }}, new HashMap<>());

        StreamProcessing sp = new StreamProcessing(1, Duration.ofSeconds(5), insertableStreamTable);
        String[] hashBy = new String[]{"firstPartitionByColumn", "secondPartitionByColumn"};
        SessionWindow sessionWindow = new SessionWindow(Duration.ofMillis(10),
                hashBy,
                "ts",
                new TimeWindowFunction() {
                    @Override
                    public List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                        mapMap.get(partitionByColumns).put(new ArrayList<Long>(2){{
                            add(windowStart);
                            add(windowEnd);
                        }}, rows.size());

                        List<Comparable[]> comparablesList = new ArrayList<>(rows.size());
                        for (Row row : rows) {
                            comparablesList.add(new Comparable[]{windowStart, windowEnd});
                        }

                        return comparablesList;
                    }
                }, "windowStart", "windowEnd");
        Rehash rehash = sp.rehash("rehash1", hashBy);

        List<Table> tables = new ArrayList<>();
        sp.compute(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                Table table = insertableStreamTable.consume();
                List<Table> tableList = rehash.rehash(table, myThreadIndex);
                table = sessionWindow.session(tableList);
                if (table.size() > 0) {
                    tables.add(table);
                }
            }
        });

        // watermark默认1000ms导致上面insertableStreamTable中的数据是一次计算的但最后一条time=103的数据由于windowTimeout时间
        // 没到没有计算,因此第一个表中的数据是5条
        assert tables.get(0).size() == 5;
        assert (long) tables.get(0).getColumn("windowEnd").get(3) == 14L;
        assert (long) tables.get(0).getColumn("windowEnd").get(4) == 24L;
        // 第二个表中的数据是time=103的那条在2000ms noDataDelay之后windowTimeout超时触发
        assert tables.get(1).size() == 1;
        assert (long) tables.get(1).getColumn("windowEnd").get(0) == 104L;

        assert mapMap.get(new ArrayList<Comparable>(2){{
            add(new ByteArray("firstPartitionByColumn1"));
            add(new ByteArray("secondPartitionByColumn1"));
        }}).get(new ArrayList<Long>(2){{
            add(3L);
            add(14L);
        }}).equals(4);

        assert mapMap.get(new ArrayList<Comparable>(2){{
            add(new ByteArray("firstPartitionByColumn1"));
            add(new ByteArray("secondPartitionByColumn1"));
        }}).get(new ArrayList<Long>(2){{
            add(23L);
            add(24L);
        }}).equals(1);

        assert mapMap.get(new ArrayList<Comparable>(2){{
            add(new ByteArray("firstPartitionByColumn2"));
            add(new ByteArray("secondPartitionByColumn2"));
        }}).get(new ArrayList<Long>(2){{
            add(103L);
            add(104L);
        }}).equals(1);
    }
}