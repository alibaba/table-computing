package com.alibaba.jstream.criteria;

import com.alibaba.jstream.table.Row;

public interface Criteria {
    boolean filter(Row row);
}
