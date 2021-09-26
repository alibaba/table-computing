package com.alibaba.jstream.sp.input;

import com.alibaba.jstream.table.Table;
import com.alibaba.jstream.table.Type;
import com.alibaba.sdb.jdbc.SdbDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class SdbFetcher extends JdbcFetcher {
    private final String url;
    private final Properties properties;
    private final SdbDriver sdbDriver;

    public SdbFetcher(String address,
                      String catalog,
                      String schema,
                      String userName,
                      String password,
                      Map<String, Type> columnTypeMap) {
        super(columnTypeMap, null);
        Properties properties = new Properties();
        properties.setProperty("user", userName);
        properties.setProperty("password", password);
        this.url = "jdbc:sdb://" + address + "/" + catalog + "/" + schema;
        this.properties = properties;
        this.sdbDriver = new SdbDriver();
    }

    public Table fetch(String sql) throws SQLException {
        connection = sdbDriver.connect(url, properties);
        Table table = super.fetch(sql);
        connection.close();
        return table;
    }
}
