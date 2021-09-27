package com.alibaba.tc.window;

import com.alibaba.tc.table.ColumnTypeBuilder;
import com.alibaba.tc.table.Row;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.TableBuilder;
import com.alibaba.tc.table.Type;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.alibaba.tc.window.StoreType.STORE_BY_COLUMN;

public class TimeWindowTest {

    private static class TimeWindowForTest extends TimeWindow {
        protected TimeWindowForTest(String timeColumnName) {
            super(STORE_BY_COLUMN, timeColumnName);
        }

        @Override
        public List<Row> getRows(List<Comparable> partitionBy) {
            return null;
        }
    }

    @Test
    public void testWatermark() {
        Map<String, Type> columnTypeMap = new ColumnTypeBuilder().
                column("ts", Type.BIGINT).
                build();
        TimeWindowForTest timeWindowForTest = new TimeWindowForTest("ts");
        timeWindowForTest.setWatermark(Duration.ofMillis(100));
        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
        tableBuilder.append(0, 3L);
        tableBuilder.append(0, 3L);
        tableBuilder.append(0, 5L);
        List<Table> tableList = timeWindowForTest.watermark(tableBuilder.build());
        assert tableList.isEmpty();

        tableBuilder = new TableBuilder(columnTypeMap);
        tableBuilder.append(0, 100L);
        tableBuilder.append(0, 101L);
        tableList = timeWindowForTest.watermark(tableBuilder.build());
        assert tableList.isEmpty();

        tableBuilder = new TableBuilder(columnTypeMap);
        tableBuilder.append(0, 105L);
        tableList = timeWindowForTest.watermark(tableBuilder.build());
        assert tableList.size() == 2;
        assert tableList.get(0).size() == 2;
        assert tableList.get(1).size() == 1;

        timeWindowForTest.setWatermark(Duration.ofMillis(0));
        tableBuilder = new TableBuilder(columnTypeMap);
        tableBuilder.append(0, 105L);
        tableBuilder.append(0, 109L);
        tableBuilder.append(0, 109L);
        tableList = timeWindowForTest.watermark(tableBuilder.build());
        assert tableList.size() == 4;
        assert tableList.get(3).size() == 2;
    }
}