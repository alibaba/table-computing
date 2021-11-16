/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.tc.state.memdb;

import com.alibaba.sdb.spi.Page;
import com.alibaba.sdb.spi.block.Block;
import com.alibaba.sdb.spi.block.ByteArrayBlock;
import com.alibaba.sdb.spi.block.DictionaryBlock;
import com.alibaba.sdb.spi.block.Int128ArrayBlock;
import com.alibaba.sdb.spi.block.VariableWidthBlock;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.alibaba.sdb.spi.type.BooleanType;
import com.alibaba.sdb.spi.type.DecimalType;
import com.alibaba.sdb.spi.type.Type;
import com.alibaba.sdb.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NsdbTablePagesStoreWithMerge
{
    @GuardedBy("this")
    private long currentBytes;

    private final NsdbIndexes indexes = new NsdbIndexes();
    private final TableData tableData = new TableData();
    private NsdbTableHandle tableHandle;
    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();

    //这个值不能太大也不能太小。太大会导致多次 growCapacity 多次的内存拷贝并且额外50%的内存浪费，太小会导致CPU Cache命中率降低影响性能
    //12442 = (64 growCapacity 13次)
    //见BlockUtil.calculateNewArraySize long newSize = (long) currentSize + (currentSize >> 1);
    private static final int MAX_PAGE_POSITION_COUNT = 12442;
    private static final int MIN_PAGE_SIZE = 1000;

    private static final boolean[] nulls = new boolean[MAX_PAGE_POSITION_COUNT];
    static {
        for (int j = 0; j < MAX_PAGE_POSITION_COUNT; j++) {
            nulls[j] = true;
        }
    }

    private static final Optional<boolean[]> optionalNulls = Optional.of(nulls);
    private static final ByteArrayBlock nullBooleans = new ByteArrayBlock(MAX_PAGE_POSITION_COUNT, optionalNulls, new byte[MAX_PAGE_POSITION_COUNT]);
    private static final VariableWidthBlock nullVarchars = new VariableWidthBlock(MAX_PAGE_POSITION_COUNT, Slices.EMPTY_SLICE, new int[MAX_PAGE_POSITION_COUNT + 1], optionalNulls);
    private static final Int128ArrayBlock nullDecimals = new Int128ArrayBlock(MAX_PAGE_POSITION_COUNT, optionalNulls, new long[MAX_PAGE_POSITION_COUNT * 2]);
    private long smallPageBeginMillis = -1;
    private long smallPageRows = 0;

    public NsdbTablePagesStoreWithMerge(NsdbTableHandle tableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    public void resetTableHandle(NsdbTableHandle tableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    public int getPagesCount()
    {
        return (int) readLock(() -> {
            return tableData.getPages().size();
        });
    }

    public long getEarliestTime()
    {
        return (long) readLock(() -> {
            return tableData.getFirstTime();
        });
    }

    public long expire()
    {
        return (long) writeLock(() -> {
            if (tableData.pages.isEmpty()) {
                throw new IllegalStateException(format("No page has been expired, table: %s, firstTime: %d, pages: %d",
                        tableHandle.getTableName(),
                        tableData.getFirstTime(),
                        tableData.getPages().size()));
            }

            long size = tableData.expire(indexes);
            currentBytes -= size;

            return size;
        });
    }

    public void createIndex(List<NsdbColumnHandle> columnHandles)
    {
        writeLock(() -> {
            indexes.createIndex(columnHandles);

            Map<Integer, Page> pages = tableData.getPages();
            for (Integer pageIndex : pages.keySet()) {
                indexes.insertIndex(pages.get(pageIndex), pageIndex);
            }

            return null;
        });
    }

    public long add(final Page pageSrc)
    {
        return (long) writeLock(() -> {
            Page page = pageSrc;
            long curMillis = System.currentTimeMillis();
            if (page.getPositionCount() < MIN_PAGE_SIZE && -1 == smallPageBeginMillis) {
                smallPageBeginMillis = curMillis;
            }
            if (smallPageBeginMillis >= 0) {
                smallPageRows += page.getPositionCount();
            }

            long preBytes = currentBytes;
            page.compactToOffheap();
            if (smallPageRows >= MIN_PAGE_SIZE) {
                page = mergePages(page);
//                System.gc();
                smallPageRows = 0;
                smallPageBeginMillis = -1;
            }
            if (indexes.hasIndex()) {
                indexes.insertIndex(page, tableData.getPageIndex());
            }
            tableData.add(page, curMillis);
            currentBytes += page.getRetainedSizeInBytes();
            return currentBytes - preBytes;
        });
    }

    private Page mergePages(Page pageNew)
    {
        ImmutableList<Long> millis = tableData.getMillis(smallPageBeginMillis);
        List<Page> pages = new ArrayList<>();
        for (Long ms : millis) {
            List<Integer> pageIndexes = tableData.removePageIndexes(ms);
            for (Integer pageIndex : pageIndexes) {
                Page page = tableData.removePage(pageIndex);
                if (indexes.hasIndex()) {
                    indexes.deleteIndex(page, pageIndex);
                }
                currentBytes -= page.getRetainedSizeInBytes();

                pages.add(page);
            }
        }
        pages.add(pageNew);

        return Page.mergePages(pages);
    }

    public List<Page> getPages(
            List<Integer> columnIndexes,
            TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        return (List<Page>) readLock(() -> {
            ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();
            NsdbIndex index = indexes.chooseOptimizedIndex(effectivePredicate);
            if (null != index) {
                partitionedPages.addAll(useIndex(effectivePredicate, index, columnIndexes));
            }
            else {
                for (Integer pageIndex : tableData.getPages().keySet()) {
                    Page page = tableData.getPages().get(pageIndex);
                    partitionedPages.add(getColumns(page, columnIndexes));
                }
            }
            return partitionedPages.build();
        });
    }

    private List<Page> useIndex(TupleDomain<NsdbColumnHandle> effectivePredicate, NsdbIndex index, List<Integer> columnIndexes)
    {
        Map<Integer, List<Integer>> positions = index.use1LevelIndex(effectivePredicate);
        Map<Integer, Page> pages = tableData.getPages();
        List<Page> retPages = new ArrayList<>();
        for (Integer pageIndex : positions.keySet()) {
            List<Integer> positionsInPage = positions.get(pageIndex);
            int[] ids = new int[positionsInPage.size()];
            int j = 0;
            for (Integer position : positionsInPage) {
                ids[j++] = position;
            }

            Page page = pages.get(pageIndex);
            Block[] blocks = new Block[columnIndexes.size()];
            for (int i = 0; i < columnIndexes.size(); i++) {
                Integer columnIndex = columnIndexes.get(i);
                blocks[i] = new DictionaryBlock(page.getBlock(columnIndex), ids);
            }
            retPages.add(new Page(blocks));
        }

        return retPages;
    }

    public long getCurrentBytes()
    {
        return (long) readLock(() -> {
            return currentBytes;
        });
    }

    private Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        int positionCount = page.getPositionCount();

        for (int i = 0; i < columnIndexes.size(); i++) {
            Integer columnIndex = columnIndexes.get(i);
            if (columnIndex >= page.getChannelCount()) {
                outputBlocks[i] = nullBlock(positionCount, columnIndex);
            }
            else {
                outputBlocks[i] = page.getBlock(columnIndexes.get(i));
            }
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private Block nullBlock(int positionCount, Integer columnIndex)
    {
        Type type = tableHandle.getColumnType(columnIndex);
        if (type instanceof BooleanType) {
            if (positionCount == MAX_PAGE_POSITION_COUNT) {
                return nullBooleans;
            }
            else {
                return new ByteArrayBlock(positionCount, optionalNulls, new byte[positionCount]);
            }
        }
        else if (type instanceof VarcharType) {
            if (positionCount == MAX_PAGE_POSITION_COUNT) {
                return nullVarchars;
            }
            else {
                return new VariableWidthBlock(positionCount, Slices.EMPTY_SLICE, new int[positionCount + 1], optionalNulls);
            }
        }
        else if (type instanceof DecimalType) {
            if (positionCount == MAX_PAGE_POSITION_COUNT) {
                return nullDecimals;
            }
            else {
                return new Int128ArrayBlock(positionCount, optionalNulls, new long[positionCount * 2]);
            }
        }
        else {
            throw new IllegalStateException("unknown column type");
        }
    }

    public long getRows()
    {
        return (long) readLock(() -> {
            return tableData.getRows();
        });
    }

    private Object writeLock(Supplier supplier)
    {
        Object object;
        reentrantReadWriteLock.writeLock().lock();
        try {
            object = supplier.get();
        }
        finally {
            reentrantReadWriteLock.writeLock().unlock();
        }
        return object;
    }

    private Object readLock(Supplier supplier)
    {
        Object object;
        reentrantReadWriteLock.readLock().lock();
        try {
            object = supplier.get();
        }
        finally {
            reentrantReadWriteLock.readLock().unlock();
        }

        return object;
    }

    private static final class TableData
    {
        private final Map<Integer, Page> pages = new HashMap<>();
        private final TreeMap<Long, List<Integer>> msPage = new TreeMap<>();
        private long rows;
        private int pageIndex;

        private long getFirstTime()
        {
            if (pages.isEmpty()) {
                return Long.MAX_VALUE;
            }

            return msPage.firstKey();
        }

        private void add(Page page, long curMillis)
        {
            pages.put(pageIndex, page);
            List<Integer> pageIndexes = msPage.get(curMillis);
            if (null == pageIndexes) {
                msPage.put(curMillis, new ArrayList<Integer>(){{add(pageIndex);}});
            }
            else {
                pageIndexes.add(pageIndex);
            }
            pageIndex = circle(pageIndex);
            rows += page.getPositionCount();
        }

        private List<Integer> removePageIndexes(long ms)
        {
            return msPage.remove(ms);
        }

        private Page removePage(int pageIndex)
        {
            Page page = pages.remove(pageIndex);
            rows -= page.getPositionCount();
            return page;
        }

        private int circle(int i)
        {
            if (i == Integer.MAX_VALUE) {
                return 0;
            }
            else {
                return ++i;
            }
        }

        private long expire(NsdbIndexes indexes)
        {
            if (pages.isEmpty()) {
                return 0L;
            }

            long size = 0;
            List<Integer> pageIndexes = msPage.remove(msPage.firstKey());
            for (Integer pageIndex : pageIndexes) {
                Page page = pages.remove(pageIndex);
                indexes.deleteIndex(page, pageIndex);
                rows -= page.getPositionCount();
                size += page.getRetainedSizeInBytes();
            }

            return size;
        }

        private ImmutableList<Long> getMillis(long greaterOrEqualThan)
        {
            return ImmutableList.copyOf(msPage.tailMap(greaterOrEqualThan, true).keySet());
        }

        private Map<Integer, Page> getPages()
        {
            return pages;
        }

        private long getRows()
        {
            return rows;
        }

        private int getPageIndex()
        {
            return pageIndex;
        }
    }
}
