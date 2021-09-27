package com.alibaba.tc.sp.dimension;

import com.alibaba.tc.table.Type;

import java.time.Duration;
import java.util.Map;

public class MysqlDimensionTable extends RdsDimensionTable {
    public MysqlDimensionTable(String jdbcUrl,
                               String tableName,
                               String userName,
                               String password,
                               Duration refreshInterval,
                               Map<String, Type> columnTypeMap,
                               String... primaryKeyColumnNames) {
        super(jdbcUrl, tableName, userName, password, refreshInterval, columnTypeMap, primaryKeyColumnNames);
    }

    public MysqlDimensionTable(String jdbcUrl,
                               String userName,
                               String password,
                               Duration refreshInterval,
                               String sql,
                               Map<String, Type> columnTypeMap,
                               String... primaryKeyColumnNames) {
        super(jdbcUrl, userName, password, refreshInterval, sql, columnTypeMap, primaryKeyColumnNames);
    }
}
