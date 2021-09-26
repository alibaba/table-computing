package com.alibaba.jstream;

import org.junit.Test;

import static com.alibaba.jstream.ArrayUtil.bytesToInt;
import static com.alibaba.jstream.ArrayUtil.intToBytes;

public class ArrayUtilTest {

    @Test
    public void testIntToBytes() {
        assert bytesToInt(intToBytes(-1)) == -1;
        assert bytesToInt(intToBytes(0)) == 0;
        assert bytesToInt(intToBytes(1)) == 1;
        assert bytesToInt(intToBytes(40000)) == 40000;
    }
}