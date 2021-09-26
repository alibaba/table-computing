package com.alibaba.jstream.util;

import com.alibaba.jstream.table.Row;

import java.util.List;

public class AggregationUtil {
    public static String groupConcat(List<Row> rows, String columnName) {
        StringBuilder sb = new StringBuilder();
        for (Row row : rows) {
            sb.append(row.get(columnName));
            sb.append(',');
        }
        return sb.length() > 0 ? sb.deleteCharAt(sb.length() - 1).toString() : sb.toString();
    }

    public static int sumInt(List<Row> rows, String columnName) {
        int ret = 0;
        for (Row row : rows) {
            Integer i = (Integer) row.get(columnName);
            if (null == i) {
                continue;
            }
            ret += i;
        }

        return ret;
    }

    public static double sumLong(List<Row> rows, String columnName) {
        double ret = 0;
        for (Row row : rows) {
            Long l = (Long) row.get(columnName);
            if (null == l) {
                continue;
            }
            ret += l;
        }

        return ret;
    }

    public static double sumDouble(List<Row> rows, String columnName) {
        double ret = 0;
        for (Row row : rows) {
            Number n = (Number) row.get(columnName);
            if (null == n) {
                continue;
            }
            ret += n.doubleValue();
        }

        return ret;
    }

    public static Comparable max(List<Row> rows, String columnName) {
        Comparable ret = null;
        for (Row row : rows) {
            Comparable comparable = row.get(columnName);
            if (null == comparable) {
                continue;
            }
            if (null == ret) {
                ret = comparable;
                continue;
            }
            ret = comparable.compareTo(ret) > 0 ? comparable : ret;
        }

        return ret;
    }

    public static double avg(List<Row> rows, String columnName) {
        return sumDouble(rows, columnName) / rows.size();
    }
}
