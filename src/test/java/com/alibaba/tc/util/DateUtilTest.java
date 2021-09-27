package com.alibaba.tc.util;

import org.junit.Test;

import java.text.ParseException;

import static com.alibaba.tc.util.DateUtil.parseDateWithZone;

public class DateUtilTest {
    @Test
    public void test() throws ParseException {
        long ts = parseDateWithZone("2021-08-19 23:10:20 EDT");
        assert ts == 1629429020000L;
    }
}