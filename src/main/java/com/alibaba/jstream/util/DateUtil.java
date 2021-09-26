package com.alibaba.jstream.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    public static long parseDateWithZone(String date) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse(date).getTime();
    }

    public static long parseDate(String date) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime();
    }

    public static long parseDate(String pattern, String date) throws ParseException {
        return new SimpleDateFormat(pattern).parse(date).getTime();
    }

    public static String toDate(long ms, String format) {
        return new SimpleDateFormat(format).format(new Date(ms));
    }
}
