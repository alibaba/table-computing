package com.alibaba.tc.state.memdb;

import com.alibaba.sdb.plugin.offheap.LongDecimalOffheap;
import com.alibaba.sdb.plugin.offheap.LongOffheap;
import com.alibaba.sdb.plugin.offheap.Offheap;
import com.alibaba.sdb.plugin.offheap.SliceOffheap;
import com.alibaba.sdb.spi.predicate.Domain;
import com.alibaba.sdb.spi.predicate.Range;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.alibaba.sdb.spi.predicate.ValueSet;
import com.alibaba.sdb.spi.type.Decimals;
import com.alibaba.sdb.spi.type.IntegerType;
import com.alibaba.sdb.spi.type.LongDecimalType;
import com.alibaba.sdb.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class NsdbIndexTest
{
    NsdbIndex nsdbIndex;
    List<NsdbColumnHandle> columnHandleList;
    List<Offheap> keys1;
    List<Offheap> keys2;
    NsdbIndex.PagePosition pagePosition1;
    NsdbIndex.PagePosition pagePosition2;

    @Test
    public void test()
    {
        columnHandleList = new ArrayList<>();
        columnHandleList.add(new NsdbColumnHandle("col0", IntegerType.INTEGER, 0));
        columnHandleList.add(new NsdbColumnHandle("col1", VarcharType.VARCHAR, 1));
        columnHandleList.add(new NsdbColumnHandle("col2", LongDecimalType.DECIMAL, 2));
        nsdbIndex = new NsdbIndex(columnHandleList);

        testInsert1LevelIndex();
        testUse1LevelIndex1();
        testUse1LevelIndex2();
        testUse1LevelIndex3();
        testDelete1LevelIndex();
    }

    public void testInsert1LevelIndex() {
        keys1 = new ArrayList<>();
        keys1.add(null);
        keys1.add(new SliceOffheap(Slices.utf8Slice("varchar varchar")));
        BigDecimal bigDecimal = new BigDecimal("5.8").setScale(LongDecimalType.DECIMAL.getScale(), BigDecimal.ROUND_DOWN);
        Slice value = Decimals.encodeScaledValue(bigDecimal);
        keys1.add(new LongDecimalOffheap(value));
        pagePosition1 = new NsdbIndex.PagePosition(2, 4);
        nsdbIndex.insert1LevelIndex(keys1, pagePosition1);

        keys2 = new ArrayList<>();
        keys2.add(new LongOffheap(-11L));
        keys2.add(null);
        bigDecimal = new BigDecimal("6.4").setScale(LongDecimalType.DECIMAL.getScale(), BigDecimal.ROUND_DOWN);
        value = Decimals.encodeScaledValue(bigDecimal);
        keys2.add(new LongDecimalOffheap(value));
        pagePosition2 = new NsdbIndex.PagePosition(7, 9);
        nsdbIndex.insert1LevelIndex(keys2, pagePosition2);
    }

    public void testDelete1LevelIndex()
    {
        nsdbIndex.delete1LevelIndex(keys1, pagePosition1);
        nsdbIndex.delete1LevelIndex(keys2, pagePosition2);
        assertTrue(nsdbIndex.isEmpty());
    }

    public void testUse1LevelIndex1()
    {
        List<TupleDomain.ColumnDomain<NsdbColumnHandle>> columnDomains = new ArrayList<>();
        Domain domain = Domain.onlyNull(IntegerType.INTEGER);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(0), domain));
        domain = Domain.all(VarcharType.VARCHAR);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(1), domain));
        TupleDomain<NsdbColumnHandle> effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));
        Map<Integer, List<Integer>> pagePositions = nsdbIndex.use1LevelIndex(effectivePredicate);
        assertEquals(pagePositions.get(2).contains(4), true);
    }

    public void testUse1LevelIndex2()
    {
        List<TupleDomain.ColumnDomain<NsdbColumnHandle>> columnDomains = new ArrayList<>();
        Range range = Range.range(IntegerType.INTEGER, -111L, true, 500L, false);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain domain = Domain.create(valueSet, true);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(0), domain));
        TupleDomain<NsdbColumnHandle> effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));
        int cols = nsdbIndex.canUseIndex(effectivePredicate);
        assertEquals(cols, 1);

        range = Range.range(VarcharType.VARCHAR, Slices.utf8Slice("var"), true, Slices.utf8Slice("war"), true);
        valueSet = ValueSet.ofRanges(range);
        domain = Domain.create(valueSet, true);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(1), domain));
        effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));

        Map<Integer, List<Integer>> pagePositions = nsdbIndex.use1LevelIndex(effectivePredicate);
        assertEquals(pagePositions.get(2).contains(4), true);
        assertEquals(pagePositions.get(7).contains(9), true);
    }

    public void testUse1LevelIndex3()
    {
        List<TupleDomain.ColumnDomain<NsdbColumnHandle>> columnDomains = new ArrayList<>();
        Range range = Range.range(IntegerType.INTEGER, -111L, true, 500L, false);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain domain = Domain.create(valueSet, true);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(0), domain));
        TupleDomain<NsdbColumnHandle> effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));
        int cols = nsdbIndex.canUseIndex(effectivePredicate);
        assertEquals(cols, 1);

        range = Range.range(VarcharType.VARCHAR, Slices.utf8Slice("var"), true, Slices.utf8Slice("war"), true);
        valueSet = ValueSet.ofRanges(range);
        domain = Domain.create(valueSet, true);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(1), domain));

        BigDecimal bigDecimal1 = new BigDecimal("5.8").setScale(LongDecimalType.DECIMAL.getScale(), BigDecimal.ROUND_DOWN);
        Slice value1 = Decimals.encodeScaledValue(bigDecimal1);

        BigDecimal bigDecimal2 = new BigDecimal("6.4").setScale(LongDecimalType.DECIMAL.getScale(), BigDecimal.ROUND_DOWN);
        Slice value2 = Decimals.encodeScaledValue(bigDecimal2);

        range = Range.range(LongDecimalType.DECIMAL, value1, true, value2, true);
        valueSet = ValueSet.ofRanges(range);
        domain = Domain.create(valueSet, true);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(2), domain));
        effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));

        Map<Integer, List<Integer>> pagePositions = nsdbIndex.use1LevelIndex(effectivePredicate);
        assertEquals(pagePositions.get(2).contains(4), true);
        assertEquals(pagePositions.get(7).contains(9), true);
    }
}
