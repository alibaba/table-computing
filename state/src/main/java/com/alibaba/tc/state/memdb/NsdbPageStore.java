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
import com.alibaba.sdb.spi.SdbException;
import com.alibaba.sdb.spi.block.InternalUnsafe;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.facebook.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.sdb.spi.block.InternalUnsafe.directMemoryUsed;
import static com.alibaba.sdb.spi.block.InternalUnsafe.indexMemorySize;
import static com.alibaba.sdb.spi.block.InternalUnsafe.maxDirectMemory;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static java.lang.String.format;

@ThreadSafe
public class NsdbPageStore
{
    private static final Logger log = Logger.get(NsdbPageStore.class);

    private long currentBytes;

    private final Map<Long, NsdbTablePageStore> tables = new HashMap<>();
    private final NsdbMetadata nsdbMetadata;
    private final long splitsPerWorker;
    private final ScheduledExecutorService expireExecutor;
    private final ScheduledExecutorService removeTableExecutor;

    public NsdbPageStore(long splitsPerWorker, NsdbMetadata nsdbMetadata)
    {
        this.splitsPerWorker = splitsPerWorker;
        this.nsdbMetadata = nsdbMetadata;
        expireExecutor = Executors.newSingleThreadScheduledExecutor(threadsNamed("expire-nsdb-pages"));
        removeTableExecutor = Executors.newSingleThreadScheduledExecutor(threadsNamed("remove-table"));
    }

    public void start()
    {
        expireExecutor.scheduleWithFixedDelay(() -> {
            try {
                expirePages();
            }
            catch (Throwable e) {
                log.error(e, "Error expiring nsdb pages");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private synchronized NsdbTablePageStore earliestTable()
    {
        long minTs = Long.MAX_VALUE;
        NsdbTablePageStore earliestTable = null;
        for (Long tableId : tables.keySet()) {
            NsdbTablePageStore tablePagesStore = tables.get(tableId);
            if (tablePagesStore.getEarliestTime() < minTs &&
                    tablePagesStore.getPagesCount() > 1 &&
                    tablePagesStore.has2FullPage()) {
                earliestTable = tables.get(tableId);
                minTs = earliestTable.getEarliestTime();
            }
        }

        return earliestTable;
    }

    private void expirePages()
    {
        long dataSizeEverySplit = (maxDirectMemory() - indexMemorySize() - directMemoryUsed()) / splitsPerWorker;
        if (currentBytes > (dataSizeEverySplit * 70L / 100L)) {
            while (currentBytes > (dataSizeEverySplit * 70L / 100L)) {
                NsdbTablePageStore earliestTable = earliestTable();
                if (null == earliestTable) {
                    break;
                }
                long size = earliestTable.expire();
                synchronized (this) {
                    currentBytes -= size;
                }
            }

//            System.gc();
        }
    }

    public void createIndex(long tableId, List<NsdbColumnHandle> columnHandles)
    {
        getTable(tableId).createIndex(columnHandles);
    }

    private synchronized NsdbTablePageStore getTable(long tableId)
    {
        if (!contains(tableId)) {
            throw new SdbException(NsdbErrorCode.MISSING_DATA, format("Failed to find table on a worker, tableId: %d", tableId));
        }
        return tables.get(tableId);
    }

    public Page getPageForWriting(Long tableId)
    {
        NsdbTablePageStore.PageIncrSize pageIncrSize = getTable(tableId).getPageForWriting();
        currentBytes += pageIncrSize.getIncrSize();
        return pageIncrSize.getPage();
    }

    public void add(Long tableId, Page page)
    {
        if (InternalUnsafe.directMemoryUsed() + InternalUnsafe.getUsedMemory() >= maxDirectMemory()) {
            expirePages();
        }

        long size = getTable(tableId).add(page);
        synchronized (this) {
            currentBytes += size;
        }
    }

    public List<Page> getPages(
            Long tableId,
            List<Integer> columnIndexes,
            TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        return getTable(tableId).getPages(columnIndexes, effectivePredicate);
    }

    public synchronized void resetTableId(long oldTableId, NsdbTableHandle newTableHandle)
    {
        if (!contains(oldTableId)) {
            throw new SdbException(NsdbErrorCode.MISSING_DATA, format("Failed to find old tableId: %d", oldTableId));
        }

        NsdbTablePageStore tablePagesStore = tables.get(oldTableId);
        tablePagesStore.resetTableHandle(newTableHandle);
    }

//    public synchronized void resetTableId(long oldTableId, long newTableId, NsdbTableHandle newTableHandle)
//    {
//        if (!contains(oldTableId)) {
//            throw new SdbException(NsdbErrorCode.MISSING_DATA, format("Failed to find old tableId: %d", oldTableId));
//        }
//        if (!contains(newTableId)) {
//            throw new SdbException(NsdbErrorCode.MISSING_DATA, format("Failed to find new tableId: %d", newTableId));
//        }
//
//        currentBytes -= tables.remove(newTableId).getCurrentBytes();
//        NsdbTablePageStore tablePagesStore = tables.remove(oldTableId);
//        tablePagesStore.resetTableHandle(newTableHandle);
//        tables.put(newTableId, tablePagesStore);
//    }

    public void  removeTableId(long tableId)
    {
        //分布式情况下无法预知其它节点什么时候不再使用这张表了，借鉴TCP close的做法2分钟后再物理删除
        NsdbPageStore that = this;
        removeTableExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                synchronized (that) {
                    currentBytes -= tables.remove(tableId).getCurrentBytes();
                }
            }
        }, 2, TimeUnit.MINUTES);
    }

    public synchronized void initialize(long tableId, NsdbTableHandle tableHandle)
    {
        if (!contains(tableId)) {
            tables.put(tableId, new NsdbTablePageStore(tableHandle));
        }
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when NsdbPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which NsdbTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            currentBytes = 0L;
            tables.clear();
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, NsdbTablePageStore>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, NsdbTablePageStore> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (/*tableId < latestTableId && */!activeTableIds.contains(tableId)) {
                currentBytes -= tablePagesEntry.getValue().getCurrentBytes();
                tableDataIterator.remove();
            }
        }
    }

    public long getRows(long tableId)
    {
        return getTable(tableId).getRows();
    }
}
