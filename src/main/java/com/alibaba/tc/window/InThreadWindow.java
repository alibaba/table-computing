package com.alibaba.tc.window;

import com.alibaba.tc.table.Row;
import com.alibaba.tc.table.SlideTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InThreadWindow {
    protected final Map<List<Comparable>, SlideTable> partitionedTables = new HashMap<>();

    List<Row> getRows(List<Comparable> partitionBy) {
        SlideTable partitionedTable = partitionedTables.get(partitionBy);
        if (null == partitionedTable) {
            return null;
        }
        return partitionedTable.rows();
    }
}
