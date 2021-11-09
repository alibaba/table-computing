package com.alibaba.tc.sp.input;

import com.alibaba.tc.exception.InconsistentColumnSizeException;
import com.alibaba.tc.exception.UnknownTypeException;
import com.alibaba.tc.table.Table;
import com.alibaba.tc.table.TableBuilder;
import com.alibaba.tc.table.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class JdbcFetcher {
    protected Connection connection;
    protected final Map<String, Type> columnTypeMap;

    protected JdbcFetcher(Map<String, Type> columnTypeMap, Connection connection) {
        this.columnTypeMap = requireNonNull(columnTypeMap);
        if (columnTypeMap.size() < 1) {
            throw new IllegalArgumentException();
        }
        this.connection = connection;
    }

    public Table fetch(String sql) throws SQLException {
        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.getMetaData().getColumnCount() != columnTypeMap.size()) {
            throw new InconsistentColumnSizeException();
        }

        while (resultSet.next()) {
            int i = 0;
            for (Type type : columnTypeMap.values()) {
                int i1 = i + 1;
                switch (type) {
                    case INT:
                        tableBuilder.append(i, resultSet.getInt(i1));
                        break;
                    case BIGINT:
                        tableBuilder.append(i, resultSet.getLong(i1));
                        break;
                    case DOUBLE:
                        tableBuilder.append(i, resultSet.getDouble(i1));
                        break;
                    case VARCHAR:
                        tableBuilder.append(i, resultSet.getString(i1));
                        break;
                    default:
                        throw new UnknownTypeException(type.name());
                }
                i++;
            }
        }
        resultSet.close();
        preparedStatement.close();

        return tableBuilder.build();
    }

    public void close() throws SQLException {
        connection.close();
    }
}
