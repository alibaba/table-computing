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
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.facebook.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@ThreadSafe
public class NsdbSplitPageStore
{
    private static final Logger log = Logger.get(NsdbSplitPageStore.class);

    private volatile long maxCachePerSplit;
    private volatile int splitsPerWorker;
    private volatile List<NsdbPageStore> pagesStoreList;
    private volatile boolean inited;
    private volatile int curStoreIndex = 0;

    //连续写10000行之后再找行数最少的PagesStore写增加CPU Cache命中率，减少寻找行数最少的PagesStore的开销
    private static final int MAX_UNINTERRUPTED_ROWS = 10000;
    private volatile int howMany;
    private volatile NsdbPageStore currentPagesStore;

    NsdbSplitPageStore()
    {
    }

    public synchronized void initialize(NsdbConfig config, NsdbMetadata nsdbMetadata)
    {
        if (inited) {
            return;
        }

        this.maxCachePerSplit = config.getMaxCachePerSplit().toBytes();
        this.splitsPerWorker = config.getSplitsPerWorder();
        pagesStoreList = new ArrayList<>(splitsPerWorker);
        for (int i = 0; i < splitsPerWorker; i++) {
            NsdbPageStore nsdbPageStore = new NsdbPageStore(splitsPerWorker, nsdbMetadata);
            nsdbPageStore.start();
            pagesStoreList.add(nsdbPageStore);
        }
        currentPagesStore = pagesStoreList.get(0);

        inited = true;
    }

    public void createIndex(long tableId, List<NsdbColumnHandle> columnHandles)
    {
        checkInited();

        try {
            for (NsdbPageStore pagesStore : pagesStoreList) {
                pagesStore.createIndex(tableId, columnHandles);
            }
        }
        catch (NsdbIndexExistedException e) {
            log.warn(e.getMessage());
        }
    }

    public void removeTableId(long tableId)
    {
        checkInited();

        for (int i = 0; i < pagesStoreList.size(); i++) {
            pagesStoreList.get(i).removeTableId(tableId);
        }
    }

    public void resetTableId(long oldTableId, NsdbTableHandle newTableHandle)
    {
        checkInited();

        for (int i = 0; i < pagesStoreList.size(); i++) {
            pagesStoreList.get(i).resetTableId(oldTableId, newTableHandle);
        }
    }
//
//    public void resetTableId(long oldTableId, long newTableId, NsdbTableHandle newTableHandle)
//    {
//        checkInited();
//
//        for (int i = 0; i < pagesStoreList.size(); i++) {
//            pagesStoreList.get(i).resetTableId(oldTableId, newTableId, newTableHandle);
//        }
//    }

    public void initialize(long tableId, NsdbTableHandle tableHandle)
    {
        checkInited();

        for (int i = 0; i < pagesStoreList.size(); i++) {
            pagesStoreList.get(i).initialize(tableId, tableHandle);
        }
    }

    public boolean contains(Long tableId)
    {
        checkInited();

        for (NsdbPageStore pagesStore : pagesStoreList) {
            if (!pagesStore.contains(tableId)) {
                return false;
            }
        }

        return true;
    }

    public void cleanUp(Set<Long> activeTableIds)
    {
        checkInited();

        for (int i = 0; i < pagesStoreList.size(); i++) {
            pagesStoreList.get(i).cleanUp(activeTableIds);
        }
    }

    private NsdbPageStore getMinRowsStore(long tableId, int pageSize)
    {
        if (howMany < MAX_UNINTERRUPTED_ROWS) {
            howMany += pageSize;
            return currentPagesStore;
        }
        else {
            howMany = 0;
        }

        NsdbPageStore ret = pagesStoreList.get(0);
        for (int i = 1; i < pagesStoreList.size(); i++) {
            if (pagesStoreList.get(i).getRows(tableId) < ret.getRows(tableId)) {
                ret = pagesStoreList.get(i);
            }
        }

        currentPagesStore = ret;

        return ret;
    }

    //该函数不需要synchronized 没必要保证多线程下的绝对公平
    private int roundRobin()
    {
        curStoreIndex = (curStoreIndex + 1) % splitsPerWorker;
        return curStoreIndex;
    }

    public Page getPageForWriting(long tableId)
    {
        int splitNumber = roundRobin();
        Page page = pagesStoreList.get(splitNumber).getPageForWriting(tableId);
        page.setSplitNumber(splitNumber);
        return page;
    }

    public void add(long tableId, Page page)
    {
        checkInited();

        if (page.getSplitNumber() >= 0) {
            pagesStoreList.get(page.getSplitNumber()).add(tableId, page);
        }
        else {
            pagesStoreList.get(roundRobin()).add(tableId, page);
        }
    }

    public List<Page> getPages(
            Long tableId,
            int partNumber,
            List<Integer> columnIndexes,
            long expectedRows,
            TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        checkInited();

//        long rows = 0;
//        for (int i = 0; i < pagesStoreList.size(); i++) {
//            rows += pagesStoreList.get(i).getRows(tableId);
//        }

//        if (rows < expectedRows) {
//            throw new SdbException(NsdbErrorCode.MISSING_DATA,
//                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, rows));
//        }

        return pagesStoreList.get(partNumber).getPages(tableId, columnIndexes, effectivePredicate);
    }

    private void checkInited()
    {
        if (!inited) {
            throw new IllegalStateException("partsPagesStore has not been initialized");
        }
    }
}
