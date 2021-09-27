package com.alibaba.tc.criteria;

import com.alibaba.tc.table.Row;

import java.util.List;

public interface JoinCriteria {
    List<Integer> theOtherRows(Row thisRow);
}
