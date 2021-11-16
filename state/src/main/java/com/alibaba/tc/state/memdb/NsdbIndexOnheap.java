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

import com.alibaba.sdb.spi.SdbException;
import com.alibaba.sdb.spi.predicate.Domain;
import com.alibaba.sdb.spi.predicate.Marker;
import com.alibaba.sdb.spi.predicate.Range;
import com.alibaba.sdb.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import static com.alibaba.sdb.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class NsdbIndexOnheap
{
    public static class PagePosition
    {
        private final int page;
        private final int position;

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

    private static class Level {
        private Set<PagePosition> values = new HashSet<>();
        private TreeMap<Comparable, Level> index = new TreeMap<>();
        private Level nullIndex = null;
    }

    private final List<NsdbColumnHandle> columnHandles;
    private final Level unifiedIndex = new Level();

    public NsdbIndexOnheap(List<NsdbColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
    }

    public boolean isEmpty()
    {
        return unifiedIndex.index.isEmpty() && unifiedIndex.values.isEmpty() && unifiedIndex.nullIndex == null;
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

    private Level lastLevel(List<Comparable> keys)
    {
        Level lastLevel = unifiedIndex;
        for (int i = 0; i < keys.size() - 1; i++) {
            Comparable key = keys.get(i);
            if (null == key) {
                if (null == lastLevel.nullIndex) {
                    lastLevel.nullIndex = new Level();
                }
                lastLevel = lastLevel.nullIndex;
            }
            else {
                Level tmp = lastLevel.index.get(key);
                if (null == tmp) {
                    tmp = new Level();
                    lastLevel.index.put(key, tmp);
                }
                lastLevel = tmp;
            }
        }

        return lastLevel;
    }

    /**
     * 插入1级索引
     * @param keys 对应每个 columnHandles 的值
     * @param pagePosition NsdbPagesStore 里的 Page 的下标 和 Page 内的 position 的下标
     */
    public void insert1LevelIndex(List<Comparable> keys, PagePosition pagePosition)
    {
        Level lastLevel = lastLevel(keys);
        Comparable key = keys.get(keys.size() - 1);
        if (null == key) {
            lastLevel.nullIndex.values.add(pagePosition);
        }
        else {
            Level values = lastLevel.index.get(key);
            if (null == values) {
                values = new Level();
                lastLevel.index.put(key, values);
            }
            values.values.add(pagePosition);
        }
    }

    /**
     * 删除1级索引
     * @param keys 对应每个 columnHandles 的值
     * @param pagePosition NsdbPagesStore 里的 Page 的下标 和 Page 内的 position 的下标
     */
    public void delete1LevelIndex(List<Comparable> keys, PagePosition pagePosition)
    {
        Stack<Level> stackLevel = new Stack<>();
        Stack<Comparable> stackKey = new Stack<>();
        Level lastLevel = unifiedIndex;
        for (int i = 0; i < keys.size() - 1; i++) {
            Comparable key = keys.get(i);
            stackKey.push(key);
            stackLevel.push(lastLevel);

            if (null == key) {
                lastLevel = lastLevel.nullIndex;
            }
            else {
                lastLevel = lastLevel.index.get(key);
            }
        }

        Comparable key = keys.get(keys.size() - 1);
        if (null == key) {
            lastLevel.nullIndex.values.remove(pagePosition);
            if (lastLevel.nullIndex.values.isEmpty()) {
                //将已经分配的可能很大的hash set空间释放掉
                lastLevel.nullIndex.values = new HashSet<>();
            }
        }
        else {
            Level values = lastLevel.index.get(key);
            values.values.remove(pagePosition);
            if (values.values.isEmpty()) {
                lastLevel.index.remove(key);
                if (lastLevel.index.isEmpty()) {
                    //将已经分配的可能很大的tree map空间释放掉(tree map可能不存在这个问题，for safety)
                    lastLevel.index = new TreeMap<>();
                }
            }
        }

        Level level = lastLevel;
        while (stackLevel.size() > 0) {
            lastLevel = level;
            level = stackLevel.pop();
            key = stackKey.pop();
            assert lastLevel.values.isEmpty();
            if (lastLevel.index.isEmpty() &&
                    ((lastLevel.nullIndex == null) ||
                            (lastLevel.nullIndex.values.isEmpty() && lastLevel.nullIndex.index.isEmpty()))) {
                if (key != null) {
                    level.index.remove(key);
                } else {
                    //将已经分配的可能很大的hash set和tree map空间释放掉
                    level.nullIndex = null;
                }
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
                                      Level level,
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

        if (domain.isOnlyNull()) {
            recursiveColumnRange(domainMap, level.nullIndex, pagePositions, column + 1);
            return;
        }

        if (domain.isNullAllowed()) {
            recursiveColumnRange(domainMap, level.nullIndex, pagePositions, column + 1);
        }

        TreeMap<Comparable, Level> index = level.index;
        if (index.isEmpty()) {
            return;
        }

        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        for (int i = 0; i < ranges.size(); i++) {
            Range range = ranges.get(i);

            Marker rangeLow = range.getLow();
            Marker rangeHigh = range.getHigh();
            boolean fromInclusive = rangeLow.getBound() == Marker.Bound.EXACTLY;
            boolean toInclusive = rangeHigh.getBound() == Marker.Bound.EXACTLY;

            Comparable low;
            Comparable high;
            if (rangeLow.getValueBlock().isPresent()) {
                low = (Comparable) rangeLow.getValue();
            }
            else {
                low = index.firstKey();
                fromInclusive = true;
            }
            if (rangeHigh.getValueBlock().isPresent()) {
                high = (Comparable) rangeHigh.getValue();
            }
            else {
                high = index.lastKey();
                toInclusive = true;
            }

            if (low.compareTo(index.firstKey()) < 0) {
                low = index.firstKey();
                fromInclusive = true;
            }
            if (high.compareTo(index.lastKey()) > 0) {
                high = index.lastKey();
                toInclusive = true;
            }
            if (low.compareTo(high) > 0) {
                continue;
            }

            NavigableMap<Comparable, Level> sub = level.index.subMap(
                    low,
                    fromInclusive,
                    high,
                    toInclusive
            );

            for (Level level1 : sub.values()) {
                recursiveColumnRange(domainMap, level1, pagePositions, column + 1);
            }
        }
    }

    private void addPositions(Map<Integer, List<Integer>> pagePositions, Level level, int column)
    {
        if (column > columnHandles.size() || null == level) {
            return;
        }

        for (PagePosition position : level.values) {
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

        for (Level level1 : level.index.values()) {
            addPositions(pagePositions, level1, column + 1);
        }

        addPositions(pagePositions, level.nullIndex, column + 1);
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
