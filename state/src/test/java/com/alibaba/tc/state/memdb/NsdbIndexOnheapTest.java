package com.alibaba.tc.state.memdb;

import com.alibaba.sdb.spi.predicate.Domain;
import com.alibaba.sdb.spi.predicate.Range;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.alibaba.sdb.spi.predicate.ValueSet;
import com.alibaba.sdb.spi.type.DoubleType;
import com.alibaba.sdb.spi.type.IntegerType;
import com.alibaba.sdb.spi.type.LongDecimalType;
import com.alibaba.sdb.spi.type.VarcharType;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class NsdbIndexOnheapTest
{
    NsdbIndexOnheap nsdbIndex;
    List<NsdbColumnHandle> columnHandleList;
    List<Comparable> keys1;
    List<Comparable> keys2;
    NsdbIndexOnheap.PagePosition pagePosition1;
    NsdbIndexOnheap.PagePosition pagePosition2;

    @Test
    public void test()
    {
        columnHandleList = new ArrayList<>();
        columnHandleList.add(new NsdbColumnHandle("col0", IntegerType.INTEGER, 0));
        columnHandleList.add(new NsdbColumnHandle("col1", VarcharType.VARCHAR, 1));
        columnHandleList.add(new NsdbColumnHandle("col2", LongDecimalType.DECIMAL, 2));
        nsdbIndex = new NsdbIndexOnheap(columnHandleList);

        testInsert1LevelIndex();
        testUse1LevelIndex1();
        testUse1LevelIndex2();
        testUse1LevelIndex3();
        testDelete1LevelIndex();
    }

    public void testInsert1LevelIndex() {
        keys1 = new ArrayList<>();
        keys1.add(null);
        keys1.add(Slices.utf8Slice("varchar varchar"));
        keys1.add(5.8);
        pagePosition1 = new NsdbIndexOnheap.PagePosition(2, 4);
        nsdbIndex.insert1LevelIndex(keys1, pagePosition1);

        keys2 = new ArrayList<>();
        keys2.add(-11L);
        keys2.add(null);
        keys2.add(6.4);
        pagePosition2 = new NsdbIndexOnheap.PagePosition(7, 9);
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
        effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));

        range = Range.range(DoubleType.DOUBLE, 5.8, true, 6.4, true);
        valueSet = ValueSet.ofRanges(range);
        domain = Domain.create(valueSet, true);
        columnDomains.add(new TupleDomain.ColumnDomain(columnHandleList.get(2), domain));
        effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));

        Map<Integer, List<Integer>> pagePositions = nsdbIndex.use1LevelIndex(effectivePredicate);
        assertEquals(pagePositions.get(2).contains(4), true);
        assertEquals(pagePositions.get(7).contains(9), true);
    }
}
