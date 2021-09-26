package com.alibaba.jstream;

import com.alibaba.jstream.util.IpUtil;
import com.google.common.collect.TreeMultimap;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class IpUtilTest {

    @Test
    public void getIp() {
        TreeMultimap<Integer, Integer> treeMap = TreeMultimap.create();
        treeMap.put(3, 5);
        treeMap.put(3, 6);
        treeMap.put(4, 7);
        treeMap.put(2, 7);
        for (Integer i : treeMap.values()) {
            System.out.println(i);
        }
        assertNotNull(IpUtil.getIp());
    }
}