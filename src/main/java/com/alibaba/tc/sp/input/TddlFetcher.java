package com.alibaba.tc.sp.input;

import com.alibaba.tc.table.Type;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.taobao.tddl.group.jdbc.TGroupConnection;
import com.taobao.tddl.jdbc.group.TGroupDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class TddlFetcher extends JdbcFetcher {
    public TddlFetcher(String appName,
                       String ak,
                       String sk,
                       Map<String, Type> columnTypeMap) throws SQLException {
        this(appName, ak, sk, null, columnTypeMap);
    }

    public TddlFetcher(String appName,
                       String ak,
                       String sk,
                       String unitName,
                       Map<String, Type> columnTypeMap) throws SQLException {
        super(columnTypeMap, connection(appName, ak, sk, unitName));
    }

    private static Connection connection(String appName,
                                         String ak,
                                         String sk,
                                         String unitName) throws SQLException {
        TGroupDataSource tGroupDataSource = new TGroupDataSource();
        tGroupDataSource.setDbGroupKey(appName.replaceFirst("_APP$", "_GROUP"));
        tGroupDataSource.setAppName(appName);
        tGroupDataSource.setAccessKey(ak);
        tGroupDataSource.setSecretKey(sk);
        tGroupDataSource.setUnitName(unitName);
        tGroupDataSource.init();
        return tGroupDataSource.getConnection();
    }
}
