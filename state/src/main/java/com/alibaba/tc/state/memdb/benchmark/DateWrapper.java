package com.alibaba.tc.state.memdb.benchmark;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xionghao liu  @ 2017/12/13 下午4:21.
 * xionghao.lxh@alibaba-inc.com
 */
public class DateWrapper {

    private static final String UNIT_REGEX = "[+-]\\s*\\d+[smd]";
    private static final Pattern DATE_NUM_PATTERN = Pattern.compile(UNIT_REGEX);

    private static final String VAR_REGEX = "\\$\\{[^${}]*}";
    private static final Pattern VAR_PATTERN = Pattern.compile(VAR_REGEX);

    private static final Map<String, Long> unit2LongMap = new HashMap<String, Long>() {{
        put("s", 1000L);
        put("m", 60 * 1000L);
        put("h", 60 * 60 * 1000L);
        put("d", 24 * 60 * 60 * 1000L);
    }};

    private static String getDateVarValue(String rawVarString, long ts) {
        int num = 0;
        String unit = "s";
        Matcher matcher = DATE_NUM_PATTERN.matcher(rawVarString);
        String dateFormatString = rawVarString.replaceAll("[${}]", "");
        if (matcher.find()) {
            String numString = matcher.group();
            num = Integer.valueOf(matcher.group().replaceAll("[\\ssmhd]", ""));
            unit = numString.substring(numString.length() - 1);
            dateFormatString = dateFormatString.replace(numString, "");
        }
        dateFormatString = dateFormatString.trim();
        long times = unit2LongMap.getOrDefault(unit, 1000L);
        return FastDateFormat.getInstance(dateFormatString).format(ts + num * times);
    }

    public static String wrap(String text, long ts) {
        String result = text;
        Matcher matcher = VAR_PATTERN.matcher(text);
        while (matcher.find()) {
            String varString = matcher.group(0);
            String varValue = getDateVarValue(varString, ts);
            //System.out.println(varString + " --> " + varValue);
            result = result.replace(varString, varValue);
        }
        return result;
    }

    /*public static void main(String[] args) throws ParseDateVariableErrorException {
     *//*System.out.println(getDateVarValue("${yyyy-MM-dd HH:mm:ss - 3d}", System.currentTimeMillis()));*//*
        System.out.println(wrap("${yyyy-MM-dd -2d} ${yyyy/MM/dd}", System.currentTimeMillis()));
    }*/
}
