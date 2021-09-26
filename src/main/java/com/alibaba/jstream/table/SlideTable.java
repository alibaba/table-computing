package com.alibaba.jstream.table;

import java.util.List;

public interface SlideTable {
    void addRow(Table table, int row);
    int countLessThan(long ts);
    void removeLessThan(long ts);
    void removeFirstRow();
    int size();
    List<Row> rows();
    long firstTime();
    long lastTime();
}
