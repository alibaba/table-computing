package com.alibaba.tc.criteria;

import com.alibaba.tc.table.Row;

public interface Criteria {
    boolean filter(Row row);
}
