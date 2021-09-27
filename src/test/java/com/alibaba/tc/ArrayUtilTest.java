package com.alibaba.tc;

import org.junit.Test;

import static com.alibaba.tc.ArrayUtil.bytesToInt;
import static com.alibaba.tc.ArrayUtil.intToBytes;

public class ArrayUtilTest {

    @Test
    public void testIntToBytes() {
        assert bytesToInt(intToBytes(-1)) == -1;
        assert bytesToInt(intToBytes(0)) == 0;
        assert bytesToInt(intToBytes(1)) == 1;
        assert bytesToInt(intToBytes(40000)) == 40000;
    }
}