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

    /**
     *
     * @param jdbcUrl                   jdbc url like: jdbc:mysql://localhost:3306/e-commerce
     * @param userName                  username
     * @param password                  password
     * @param refreshInterval           refresh interval
     * @param sql                       sql to select from mysql
     * @param columnTypeMap             dimension table's columns and their types build by ColumnTypeBuilder
     * @param primaryKeyColumnNames     unique primary key column names
     */
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
