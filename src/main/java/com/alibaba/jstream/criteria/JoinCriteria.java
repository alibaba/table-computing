package com.alibaba.jstream.criteria;

import com.alibaba.jstream.table.Row;

import java.util.List;

public interface JoinCriteria {
    List<Integer> theOtherRows(Row thisRow);
}
