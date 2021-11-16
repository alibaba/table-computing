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

import com.alibaba.sdb.plugin.offheap.HashCoder;
import com.alibaba.sdb.plugin.offheap.HashSetOffheap;
import com.alibaba.sdb.plugin.offheap.Offheap;
import com.alibaba.sdb.plugin.offheap.Serializer;
import com.alibaba.sdb.plugin.offheap.SkipListMapOffheap;
import com.alibaba.sdb.spi.SdbException;
import com.alibaba.sdb.spi.block.InternalUnsafe;
import com.alibaba.sdb.spi.predicate.Domain;
import com.alibaba.sdb.spi.predicate.Marker;
import com.alibaba.sdb.spi.predicate.Range;
import com.alibaba.sdb.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

import static com.alibaba.sdb.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class NsdbIndex
{
    public static class PagePosition implements Offheap<PagePosition>, HashCoder
    {
        private final int page;
        private final int position;

        @Override
        public long allocAndSerialize(int extraSize) {
            long addr = InternalUnsafe.alloc(extraSize + Integer.BYTES + Integer.BYTES);
            addr += extraSize;
            InternalUnsafe.putInt(addr, page);
            InternalUnsafe.putInt(addr + Integer.BYTES, position);

            return addr - extraSize;
        }

        @Override
        public void free(long addr, int extraSize) {
            InternalUnsafe.free(addr);
        }

        @Override
        public PagePosition deserialize(long addr)
        {
            int page = InternalUnsafe.getInt(addr);
            int position = InternalUnsafe.getInt(addr + Integer.BYTES);
            return new PagePosition(page, position);
        }

        @Override
        public int compareTo(PagePosition o) {
            if (page != o.page) {
                return page - o.page;
            }
            return position - o.position;
        }

        @Override
        public int compareTo(long addr) {
            int page = InternalUnsafe.getInt(addr);
            if (this.page != page) {
                return this.page - page;
            }
            int position = InternalUnsafe.getInt(addr + Integer.BYTES);
            if (this.position != position) {
                return this.position - position;
            }
            return 0;
        }

        @Override
        public int hashCode(long addr) {
            int page = InternalUnsafe.getInt(addr);
            int position = InternalUnsafe.getInt(addr + Integer.BYTES);
            return Objects.hash(page, position);
        }

        public PagePosition(int page, int position)
        {
            this.page = page;
            this.position = position;
        }

        public int getPage()
        {
            return page;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public String toString()
        {
            return "PagePosition{" +
                    "page=" + page +
                    ", position=" + position +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PagePosition that = (PagePosition) o;
            return page == that.page &&
                    position == that.position;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(page, position);
        }
    }

    private final List<NsdbColumnHandle> columnHandles;
    private SkipListMapOffheap unifiedIndex = new SkipListMapOffheap();

    public NsdbIndex(List<NsdbColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
    }

    public boolean isEmpty()
    {
        return unifiedIndex.isEmpty();
    }

    public boolean isThisIndex(List<NsdbColumnHandle> columnHandles)
    {
        if (this.columnHandles.size() != columnHandles.size()) {
            return false;
        }

        for (int i = 0; i < columnHandles.size(); i++) {
            if (!columnHandles.get(i).equals(this.columnHandles.get(i))) {
                return false;
            }
        }

        return true;
    }

    public List<NsdbColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    private SkipListMapOffheap lastLevel(List<Offheap> keys)
    {
        SkipListMapOffheap lastLevel = unifiedIndex;
        for (int i = 0; i < keys.size() - 1; i++) {
            Offheap key = keys.get(i);
            SkipListMapOffheap level = (SkipListMapOffheap) lastLevel.get(key);
            if (null == level) {
                level = new SkipListMapOffheap();
                lastLevel.put(key, level);
            }
            lastLevel = level;
        }

        return lastLevel;
    }

    /**
     * 插入1级索引
     * @param keys 对应每个 columnHandles 的值
     * @param pagePosition NsdbPagesStore 里的 Page 的下标 和 Page 内的 position 的下标
     */
    public void insert1LevelIndex(List<Offheap> keys, PagePosition pagePosition)
    {
        SkipListMapOffheap lastLevel = lastLevel(keys);
        Offheap key = keys.get(keys.size() - 1);
        HashSetOffheap pagePositions = (HashSetOffheap) lastLevel.get(key);
        if (null == pagePositions) {
            pagePositions = new HashSetOffheap(PagePosition.class);
            lastLevel.put(key, pagePositions);
        }
        pagePositions.add(pagePosition);
    }

    /**
     * 删除1级索引
     * @param keys 对应每个 columnHandles 的值
     * @param pagePosition NsdbPagesStore 里的 Page 的下标 和 Page 内的 position 的下标
     */
    public void delete1LevelIndex(List<Offheap> keys, PagePosition pagePosition)
    {
        Stack<SkipListMapOffheap> stackLevel = new Stack<>();
        Stack<Offheap> stackKey = new Stack<>();
        SkipListMapOffheap lastLevel = unifiedIndex;
        for (int i = 0; i < keys.size() - 1; i++) {
            Offheap key = keys.get(i);
            stackKey.push(key);
            stackLevel.push(lastLevel);

            lastLevel = (SkipListMapOffheap) lastLevel.get(key);
        }

        Offheap key = keys.get(keys.size() - 1);
        HashSetOffheap values = (HashSetOffheap) lastLevel.get(key);
        values.remove(pagePosition);
        if (values.isEmpty()) {
            lastLevel.remove(key);
        }

        SkipListMapOffheap level = lastLevel;
        while (stackLevel.size() > 0) {
            lastLevel = level;
            level = stackLevel.pop();
            key = stackKey.pop();
            if (lastLevel.isEmpty()) {
                level.remove(key);
            }
        }
    }

    /**
     * 中间某列没给定约束条件的情况下只能通过前面几列查
     * @param effectivePredicate
     * @return 满足筛选条件的所有 Page 下标和 Page 内位置
     */
    public Map<Integer, List<Integer>> use1LevelIndex(TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        Map<NsdbColumnHandle, Domain> domainMap = effectivePredicate.getDomains().get();
        Map<Integer, List<Integer>> pagePositions = new HashMap<>();
        recursiveColumnRange(domainMap, unifiedIndex, pagePositions, 0);

        return pagePositions;
    }

    private void recursiveColumnRange(Map<NsdbColumnHandle, Domain> domainMap,
                                      Serializer level,
                                      Map<Integer, List<Integer>> pagePositions,
                                      int column)
    {
        if (null == level) {
            return;
        }

        if (null == domainMap || domainMap.isEmpty()) {
            throw new SdbException(GENERIC_INTERNAL_ERROR, String.format("domain is null, domain: %s", domainMap));
        }

        if (column >= columnHandles.size()) {
            addPositions(pagePositions, level, column);
            return;
        }

        NsdbColumnHandle nsdbColumnHandle = columnHandles.get(column);
        Domain domain = domainMap.get(nsdbColumnHandle);
        if (null == domain) {
            //联合索引情况下查询条件里只有前面几个字段，到这个字段就是最后一个了
            addPositions(pagePositions, level, column);
            return;
        }

        if (level.getClass() != SkipListMapOffheap.class) {
            throw new IllegalStateException("should be SkipListMapOffheap.class");
        }

        SkipListMapOffheap skipListMapOffheap = (SkipListMapOffheap) level;
        if (skipListMapOffheap.isEmpty()) {
            return;
        }

        if (domain.isOnlyNull()) {
            recursiveColumnRange(domainMap, skipListMapOffheap.get(null), pagePositions, column + 1);
            return;
        }

        if (domain.isNullAllowed()) {
            recursiveColumnRange(domainMap, skipListMapOffheap.get(null), pagePositions, column + 1);
        }

        if (skipListMapOffheap.hasNull() && skipListMapOffheap.size() <= 1) {
            return;
        }

        Offheap firstKey = skipListMapOffheap.firstKey();
        Offheap lastKey = skipListMapOffheap.lastKey();
        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        for (int i = 0; i < ranges.size(); i++) {
            Range range = ranges.get(i);

            Marker rangeLow = range.getLow();
            Marker rangeHigh = range.getHigh();
            boolean fromInclusive = rangeLow.getBound() == Marker.Bound.EXACTLY;
            boolean toInclusive = rangeHigh.getBound() == Marker.Bound.EXACTLY;

            Offheap low;
            Offheap high;
            if (rangeLow.getValueBlock().isPresent()) {
                low = Offheap.offheaplize(rangeLow.getValue(), rangeLow.getType().getClass());
            }
            else {
                low = firstKey;
                fromInclusive = true;
            }
            if (rangeHigh.getValueBlock().isPresent()) {
                high = Offheap.offheaplize(rangeHigh.getValue(), rangeHigh.getType().getClass());
            }
            else {
                high = lastKey;
                toInclusive = true;
            }

            if (low.compareTo(firstKey) < 0) {
                low = firstKey;
                fromInclusive = true;
            }
            if (high.compareTo(lastKey) > 0) {
                high = lastKey;
                toInclusive = true;
            }
            if (low.compareTo(high) > 0) {
                continue;
            }

            SkipListMapOffheap<? extends Offheap<?>, ? extends Serializer<?>>.SubMap sub = skipListMapOffheap.subMap(
                    low,
                    fromInclusive,
                    high,
                    toInclusive
            );

            for (SkipListMapOffheap.Entry level1 : sub) {
                recursiveColumnRange(domainMap, (Serializer) level1.getValue(), pagePositions, column + 1);
            }
        }
    }

    private void addPositions(Map<Integer, List<Integer>> pagePositions, Serializer level, int column)
    {
        if (column > columnHandles.size() || null == level) {
            return;
        }

        if (level instanceof HashSetOffheap) {
            HashSetOffheap<PagePosition> hashSetOffheap = (HashSetOffheap<PagePosition>) level;
            for (PagePosition position : hashSetOffheap) {
                int page = position.getPage();
                int positionInPage = position.getPosition();
                if (pagePositions.containsKey(page)) {
                    pagePositions.get(page).add(positionInPage);
                }
                else {
                    List<Integer> positionsInPage = new ArrayList<>(1);
                    positionsInPage.add(positionInPage);
                    pagePositions.put(page, positionsInPage);
                }
            }
        }

        if (level instanceof SkipListMapOffheap) {
            SkipListMapOffheap<? extends Offheap<?>, ? extends Serializer<?>> skipListMapOffheap = (SkipListMapOffheap) level;
            for (SkipListMapOffheap.Entry<? extends Offheap<?>, ? extends Serializer<?>> entry : skipListMapOffheap) {
                addPositions(pagePositions, entry.getValue(), column + 1);
            }
            addPositions(pagePositions, skipListMapOffheap.get(null), column + 1);
        }
    }

    /**
     *
     * @param effectivePredicate 筛选条件
     * @return 满足筛选条件的所有文件指针
     */
    public List<Long> use1LevelIndexForFile(TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        return new ArrayList<>();
    }

    /**
     * 插入1级索引
     * @param values 对应每个 columnHandles 的值
     * @param position 文件指针
     */
    public void insert1LevelIndexForFile(List<Comparable> values, long position)
    {
    }

    /**
     * 插入2级索引
     * @param values 对应每个 columnHandles 的值
     * @param id Row id
     */
    public void insert2LevelIndex(List<Comparable> values, long id)
    {
    }

    public int canUseIndex(TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        Optional<Map<NsdbColumnHandle, Domain>> domains = effectivePredicate.getDomains();
        for (int i = 0; i < columnHandles.size(); i++) {
            if (!domains.get().containsKey(columnHandles.get(i))) {
                return i;
            }
        }

        return columnHandles.size();
    }
}
