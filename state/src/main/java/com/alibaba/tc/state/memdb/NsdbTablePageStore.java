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
import com.alibaba.sdb.spi.block.DictionaryBlock;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NsdbTablePageStore
{
    private static final Logger logger = Logger.get(NsdbTablePageStore.class);

    @GuardedBy("this")
    private long currentBytes;

    private final NsdbIndexes indexes = new NsdbIndexes();
    private final TableData tableData = new TableData();
    private NsdbTableHandle tableHandle;
    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();

    private static final int MIN_PAGE_SIZE = 100_000;

    private final Queue<Page> lessThanMinSizePages = new LinkedList<>();

    public NsdbTablePageStore(NsdbTableHandle tableHandle)
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

    public static class PageIncrSize
    {
        private Page page;
        private long incrSize;
        public Page getPage()
        {
            return page;
        }
        public long getIncrSize()
        {
            return incrSize;
        }
    }
    public PageIncrSize getPageForWriting()
    {
        Page page = null;
        long size = 0L;
        if (lessThanMinSizePages.isEmpty()) {
            //提到外面来，不在writeLock里做，提升并发能力
            page = new Page(MIN_PAGE_SIZE, tableHandle.getColumnHandles());
            size = page.getRetainedSizeInBytes();
        }
        final long finalSize = size;
        final Page finalPage = page;
        final PageIncrSize pageIncrSize = new PageIncrSize();
        return (PageIncrSize) writeLock(() -> {
            if (lessThanMinSizePages.isEmpty()) {
                Page pageInner = finalPage;
                long sizeInner = finalSize;
                if (null == pageInner) {
                    pageInner = new Page(MIN_PAGE_SIZE, tableHandle.getColumnHandles());
                    sizeInner = pageInner.getRetainedSizeInBytes();
                }
                currentBytes += sizeInner;

                int pageIndex = tableData.add(pageInner, System.currentTimeMillis());
                pageInner.setPageIndex(pageIndex);
                pageIncrSize.page = pageInner;
                pageIncrSize.incrSize = sizeInner;
                return pageIncrSize;
            }
            else {
                pageIncrSize.page = lessThanMinSizePages.poll();
                pageIncrSize.incrSize = 0L;
                return pageIncrSize;
            }
        });
    }

    public boolean has2FullPage()
    {
        return tableData.has2FullPage();
    }

    public long add(final Page page)
    {
        long curMillis = System.currentTimeMillis();
        if (page.getPageIndex() >= 0) {
            //来自DirectInsert的getPageForWriting then add
            int preCount = page.getPositionCount();
            int newCount = page.getNewPositionCount();
            if (newCount <= 0) {
                throw new IllegalStateException(String.format("newPositionCount: %d", newCount));
            }
            if (preCount + newCount > MIN_PAGE_SIZE) {
                throw new IndexOutOfBoundsException(String.format("positionCount: %d, newPositionCount: %d",
                        preCount,
                        newCount));
            }
            if (preCount + newCount == MIN_PAGE_SIZE) {
                page.compactOnOffheap();
            }

            final long incrSize = page.addNewPositionCount();

            writeLock(() -> {
                if (indexes.hasIndex()) {
                    indexes.insertIndex(page, page.getPageIndex(), preCount);
                }
                tableData.addRows(newCount);
                if (preCount + newCount < MIN_PAGE_SIZE) {
                    lessThanMinSizePages.add(page);
                }

                currentBytes += incrSize;
                return null;
            });

            return incrSize;
        }
        else {
            return (long) writeLock(() -> {
                tableData.addRows(page.getPositionCount());
                page.compactToOffheap();
                //来自insert into的only add
                int pageIndex = tableData.add(page, curMillis);
                if (indexes.hasIndex()) {
                    indexes.insertIndex(page, pageIndex);
                }

                currentBytes += page.getRetainedSizeInBytes();
                return page.getRetainedSizeInBytes();
            });
        }
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
                logger.warn(format("chose no index, table: %s", tableHandle.getTableName()));
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
                throw new IndexOutOfBoundsException(String.format("schema: %s, table: %s, columnIndex: %d, channelCount: %d",
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        columnIndex,
                        page.getChannelCount()));
            }

            outputBlocks[i] = page.getBlock(columnIndex);
        }

        return new Page(positionCount, outputBlocks);
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

        private void addRows(int newRows)
        {
            rows += newRows;
        }

        private int add(Page page, long curMillis)
        {
            pages.put(pageIndex, page);
            List<Integer> pageIndexes = msPage.get(curMillis);
            if (null == pageIndexes) {
                msPage.put(curMillis, new ArrayList<Integer>(){{add(pageIndex);}});
            }
            else {
                pageIndexes.add(pageIndex);
            }
            int ret = pageIndex;
            pageIndex = circle(pageIndex);

            return ret;
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

        private boolean has2FullPage()
        {
            if (pages.isEmpty()) {
                return false;
            }

            int count = 0;
            for (List<Integer> pageIndexes : msPage.values()) {
                for (Integer pageIndex : pageIndexes) {
                    Page page = pages.get(pageIndex);
                    if (page.getPageIndex() >= 0) {
                        //来自DirectInsert
                        if (page.getPositionCount() < MIN_PAGE_SIZE) {
                            continue;
                        }
                        if (page.getPositionCount() == MIN_PAGE_SIZE) {
                            count++;
                            if (count >= 2) {
                                return true;
                            }
                            continue;
                        }
                        throw new IllegalStateException(String.format("positionCount: %d", page.getPositionCount()));
                    }
                    else {
                        //来自insert into
                        return true;
                    }
                }
            }

            return false;
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

        private Map<Integer, Page> getPages()
        {
            return pages;
        }

        private long getRows()
        {
            return rows;
        }
    }
}
