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

import com.alibaba.sdb.plugin.offheap.BooleanOffheap;
import com.alibaba.sdb.plugin.offheap.DoubleOffheap;
import com.alibaba.sdb.plugin.offheap.LongDecimalOffheap;
import com.alibaba.sdb.plugin.offheap.LongOffheap;
import com.alibaba.sdb.plugin.offheap.Offheap;
import com.alibaba.sdb.plugin.offheap.SliceOffheap;
import com.alibaba.sdb.spi.Page;
import com.alibaba.sdb.spi.block.Block;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.alibaba.sdb.spi.type.BigintType;
import com.alibaba.sdb.spi.type.BooleanType;
import com.alibaba.sdb.spi.type.DoubleType;
import com.alibaba.sdb.spi.type.IntegerType;
import com.alibaba.sdb.spi.type.LongDecimalType;
import com.alibaba.sdb.spi.type.ShortDecimalType;
import com.alibaba.sdb.spi.type.SmallintType;
import com.alibaba.sdb.spi.type.TinyintType;
import com.alibaba.sdb.spi.type.Type;
import com.alibaba.sdb.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class NsdbIndexes
{
    private final List<NsdbIndex> indexList = new ArrayList<>();

    public NsdbIndexes()
    {
    }

    public synchronized void createIndex(List<NsdbColumnHandle> columnHandles)
    {
        for (NsdbIndex nsdbIndex : indexList) {
            if (nsdbIndex.isThisIndex(columnHandles)) {
                String indexFields = "";
                for (NsdbColumnHandle nsdbColumnHandle : columnHandles) {
                    indexFields += nsdbColumnHandle.getColumnName() + " ";
                }
                throw new NsdbIndexExistedException(format("already has this index: %s", indexFields));
            }
        }

        indexList.add(new NsdbIndex(columnHandles));
    }

    public boolean hasIndex()
    {
        return indexList.size() > 0;
    }

    public void deleteIndex(Page page, int pageIndex)
    {
        operateIndex(page, pageIndex, false, 0);
    }

    public void insertIndex(Page page, int pageIndex)
    {
        operateIndex(page, pageIndex, true, 0);
    }

    public void insertIndex(Page page, int pageIndex, int startPosition)
    {
        operateIndex(page, pageIndex, true, startPosition);
    }

    private void operateIndex(Page page, int pageIndex, boolean insertOrDelete, int startPosition)
    {
        for (int i = 0; i < indexList.size(); i++) {
            NsdbIndex index = indexList.get(i);
            List<NsdbColumnHandle> columnHandles = index.getColumnHandles();

            for (int positionInPage = startPosition; positionInPage < page.getPositionCount(); positionInPage++) {
                List<Offheap> indexKey = getIndexKey(page, columnHandles, positionInPage);
                if (insertOrDelete) {
                    index.insert1LevelIndex(indexKey, new NsdbIndex.PagePosition(pageIndex, positionInPage));
                }
                else {
                    index.delete1LevelIndex(indexKey, new NsdbIndex.PagePosition(pageIndex, positionInPage));
                }
            }
        }
    }

    private List<Offheap> getIndexKey(Page page, List<NsdbColumnHandle> columnHandles, int positionInPage)
    {
        ArrayList<Offheap> key = new ArrayList<>(columnHandles.size());
        for (NsdbColumnHandle columnHandle : columnHandles) {
            if (columnHandle.getColumnIndex() >= page.getChannelCount()) {
                key.add((Offheap) null);
                continue;
            }

            Block block = page.getBlock(columnHandle.getColumnIndex());
            if (block.isNull(positionInPage)) {
                key.add((Offheap) null);
                continue;
            }

            Offheap offheap;
            Type type = columnHandle.getColumnType();
            Class typeClass = type.getClass();
            if (typeClass == ShortDecimalType.class ||
                    typeClass == BigintType.class ||
                    typeClass == IntegerType.class ||
                    typeClass == SmallintType.class ||
                    typeClass == TinyintType.class) {
                offheap = new LongOffheap(type.getLong(block, positionInPage));
            }
            else if (typeClass == LongDecimalType.class) {
                offheap = new LongDecimalOffheap(type.getSlice(block, positionInPage));
            }
            else if (typeClass == BooleanType.class) {
                offheap = new BooleanOffheap(type.getBoolean(block, positionInPage));
            }
            else if (typeClass == VarcharType.class) {
                //offheap 下面可能引用着一个非常大的block，可能会成为内存泄露的隐患!!!
                offheap = new SliceOffheap(type.getSlice(block, positionInPage));
            }
            else if (typeClass == DoubleType.class) {
                offheap = new DoubleOffheap(type.getDouble(block, positionInPage));
            }
            else {
                throw new IllegalArgumentException("unknown type");
            }

            key.add(offheap);
        }

        return key;
    }

    /**
     *
     * @param effectivePredicate
     * @return null 表示没有匹配的索引可以用
     */
    public NsdbIndex chooseOptimizedIndex(TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        int max = 0;
        NsdbIndex ret = null;
        for (NsdbIndex index : indexList) {
            int columns = index.canUseIndex(effectivePredicate);
            if (columns > max) {
                max = columns;
                ret = index;
            }
        }

        return ret;
    }
}
