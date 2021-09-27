package com.alibaba.tc.window;

import com.alibaba.tc.sp.Rehash;
import com.alibaba.tc.table.ColumnTypeBuilder;
import com.alibaba.tc.table.Row;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.Type;
import com.alibaba.tc.function.TimeWindowFunction;
import com.alibaba.tc.offheap.ByteArray;
import com.alibaba.tc.sp.Compute;
import com.alibaba.tc.sp.input.InsertableStreamTable;
import com.alibaba.tc.sp.StreamProcessing;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SlideWindowTest {
    private static final Logger logger = LoggerFactory.getLogger(SlideWindowTest.class);

    @Test
    public void slide() {
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
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 24L);

        //下面这条记录触发了上面time=23的那条记录的计算（windowTimeout=10ms）
        //这条记录之后没有数据了所以要等到noDataDelay（2000ms）之后才会触发这条记录的计算
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 103L);

        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 2L);
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 301L);
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 302L);
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 303L);

        Map<List<Comparable>, Map<List<Long>, Integer>> mapMap = new HashMap<>();
        mapMap.put(new ArrayList<Comparable>(2) {{
            add(new ByteArray("firstPartitionByColumn1"));
            add(new ByteArray("secondPartitionByColumn1"));
        }}, new HashMap<>());
        mapMap.put(new ArrayList<Comparable>(2) {{
            add(new ByteArray("firstPartitionByColumn2"));
            add(new ByteArray("secondPartitionByColumn2"));
        }}, new HashMap<>());

        StreamProcessing sp = new StreamProcessing(1, insertableStreamTable);
        SlideWindow slideWindow = new SlideWindow(Duration.ofMillis(10),
                Duration.ofMillis(5),
                new String[]{"firstPartitionByColumn", "secondPartitionByColumn"},
                "ts",
                new TimeWindowFunction() {
                    @Override
                    public List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                        mapMap.get(partitionByColumns).put(new ArrayList<Long>(2) {{
                            add(windowStart);
                            add(windowEnd);
                        }}, rows.size());

                        List<Comparable[]> comparablesList = new ArrayList<>(rows.size());
                        for (Row row : rows) {
                            comparablesList.add(new Comparable[]{row.getString("firstPartitionByColumn"),
                                    row.getString("secondPartitionByColumn"),
                                    row.getLong("ts"),
                                    windowStart,
                                    windowEnd});
                        }

                        return comparablesList;
                    }
                }, "firstPartitionByColumn", "secondPartitionByColumn", "ts", "windowStart", "windowEnd");
        slideWindow.setWatermark(Duration.ofMillis(0));

        Rehash rehash = sp.rehash("rehash1", "firstPartitionByColumn", "secondPartitionByColumn");
        List<Table> tables = new ArrayList<>();
        AtomicInteger atomicInteger = new AtomicInteger();
        sp.compute(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                List<Table> tables1 = rehash.rehash(insertableStreamTable.consume(), myThreadIndex);
                Table table = slideWindow.slide(tables1);
                if (table.size() > 0) {
                    for (int i = 0; i < table.size(); i++) {
                        logger.info("{}, {}, {}, {}, {}, {}",
                                table,
                                table.getColumn(0).get(i),
                                table.getColumn(1).get(i),
                                table.getColumn(2).get(i),
                                table.getColumn(3).get(i),
                                table.getColumn(4).get(i));
                    }
                    tables.add(table);
                }
                if (15 == atomicInteger.addAndGet(table.size())) {
                    throw new InterruptedException();
                }
            }
        });

        // ts=23的那一条超出了[5,15)前进后的窗口范围[10,20) 在窗口外触发了，连同[5,15)内的3条共4条数据在同一个Table返回
        assert tables.get(1).size() == 4;
        assert (long) tables.get(1).getColumn("ts").get(3) == 23L;
        assert (long) tables.get(1).getColumn("windowEnd").get(3) == 30L;
        // ts=103的那一条超出了[15,25)前进后的窗口范围[20,30) 在窗口外触发了，连同[15,25)内的1条(ts=24那条)共2条数据在同一个Table返回
        assert tables.get(3).size() == 2;
        // ts=301的那一条超出了[20,30)前进后的窗口范围[25,35) 在窗口外触发了，连同[20,30)内的1条(ts=24那条)共2条数据在同一个Table返回
        assert tables.get(5).size() == 2;

        // ts=10的两条和ts=13的一条
        assert mapMap.get(new ArrayList<Comparable>(2) {{
            add(new ByteArray("firstPartitionByColumn1"));
            add(new ByteArray("secondPartitionByColumn1"));
        }}).get(new ArrayList<Long>(2) {{
            add(5L);
            add(15L);
        }}).equals(3);

        // ts=23的那一条超出了窗口范围[10,20) 在窗口外触发了，窗口前进到[15,25) ts=24的那一条进入了这个窗口
        assert mapMap.get(new ArrayList<Comparable>(2) {{
            add(new ByteArray("firstPartitionByColumn1"));
            add(new ByteArray("secondPartitionByColumn1"));
        }}).get(new ArrayList<Long>(2) {{
            add(15L);
            add(25L);
        }}).equals(1);

        //ts=2 的那一条
        assert mapMap.get(new ArrayList<Comparable>(2) {{
            add(new ByteArray("firstPartitionByColumn2"));
            add(new ByteArray("secondPartitionByColumn2"));
        }}).get(new ArrayList<Long>(2) {{
            add(0L);
            add(10L);
        }}).equals(1);

        //ts=301 ts=302 ts=303 的3条由于都超出了窗口范围分3次触发每次1条而不是1次触发3条
        assert mapMap.get(new ArrayList<Comparable>(2) {{
            add(new ByteArray("firstPartitionByColumn2"));
            add(new ByteArray("secondPartitionByColumn2"));
        }}).get(new ArrayList<Long>(2) {{
            add(300L);
            add(310L);
        }}).equals(1);
    }
}