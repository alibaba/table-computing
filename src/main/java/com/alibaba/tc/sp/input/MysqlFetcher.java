package com.alibaba.tc.sp.input;

import com.alibaba.tc.table.Type;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class MysqlFetcher extends JdbcFetcher {
    public MysqlFetcher(String jdbcUrl,
                        String userName,
                        String password,
                        Map<String, Type> columnTypeMap) throws SQLException {
        super(columnTypeMap, connection(jdbcUrl, userName, password));
    }

    private static Connection connection(String jdbcUrl,
                                         String userName,
                                         String password) throws SQLException {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setUser(userName);
        dataSource.setPassword(password);
        dataSource.setAutoReconnect(true);
        return dataSource.getConnection();
    }
}
