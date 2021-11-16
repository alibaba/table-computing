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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.sdb.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Integer.min;

public class NsdbIndexSingle
{
    public static class PagePosition
    {
        private final long page;
        private final int position;

        public PagePosition(long page, int position)
        {
            this.page = page;
            this.position = position;
        }

        public long getPage()
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
    private final TreeMap<List<Comparable>, Set<PagePosition>> indexTree;

    public NsdbIndexSingle(List<NsdbColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
        this.indexTree = new TreeMap(new Comparator<List<Comparable>>() {
            /**
             * null最大
             */
            @Override
            public int compare(List<Comparable> e1,
                               List<Comparable> e2)
            {
                int min = min(e1.size(), e2.size());
                for (int i = 0; i < min; i++) {
                    Comparable cmp1 = e1.get(i);
                    Comparable cmp2 = e2.get(i);

                    int cmp = compareComparable(cmp1, cmp2);

                    if (0 != cmp) {
                        return cmp;
                    }
                }
                return e1.size() - e2.size();
            }
        });
    }

    private int compareComparable(Comparable cmp1, Comparable cmp2)
    {
        if (cmp1 == null && cmp2 == null) {
            return 0;
        }
        if (cmp1 == null && cmp2 != null) {
            return 1;
        }
        if (cmp1 != null && cmp2 == null) {
            return -1;
        }

        return cmp1.compareTo(cmp2);
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

    /**
     * 插入1级索引
     * @param key 对应每个 columnHandles 的值
     * @param pagePosition NsdbPagesStore 里的 Page 的下标 和 Page 内的 position 的下标
     */
    public void insert1LevelIndex(List<Comparable> key, PagePosition pagePosition)
    {
        if (indexTree.containsKey(key)) {
            Set<PagePosition> positions = indexTree.get(key);
            positions.add(pagePosition);
        }
        else {
            Set<PagePosition> positions = new HashSet<>();
            positions.add(pagePosition);
            indexTree.put(key, positions);
        }
    }

    /**
     * 插入1级索引
     * @param key 对应每个 columnHandles 的值
     * @param pagePosition NsdbPagesStore 里的 Page 的下标 和 Page 内的 position 的下标
     */
    public void delete1LevelIndex(List<Comparable> key, PagePosition pagePosition)
    {
        if (indexTree.containsKey(key)) {
            Set<PagePosition> positions = indexTree.get(key);
            positions.remove(pagePosition);
            if (positions.isEmpty()) {
                indexTree.remove(key);
            }
        }
    }

    /**
     * 中间某列没给定约束条件的情况下只能通过前面几列查
     * @param effectivePredicate
     * @return 满足筛选条件的所有 Page 下标和 Page 内位置
     */
    public Map<Long, Set<Integer>> use1LevelIndex(TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        Map<NsdbColumnHandle, Domain> domainMap = effectivePredicate.getDomains().get();
        List<Comparable> low = new ArrayList<>();
        List<Comparable> high = new ArrayList<>();

        Map<Long, Set<Integer>> pagePositions = new HashMap<>();
        recursiveColumnRange(domainMap, indexTree, pagePositions, 0, low, high);

        return pagePositions;
    }

    private NavigableMap<List<Comparable>, Set<PagePosition>> subMapInRange(
            NavigableMap<List<Comparable>, Set<PagePosition>> map,
            List<Comparable> from,
            boolean fromInclusive,
            List<Comparable> to,
            boolean toInclusive
            )
    {
        if (null == map || map.isEmpty()) {
            return null;
        }

        List<Comparable> fromInRange = indexTree.comparator().compare(from, map.firstKey()) < 0 ? map.firstKey() : from;
        List<Comparable> toInRange = indexTree.comparator().compare(to, map.lastKey()) > 0 ? map.lastKey() : to;
        if (indexTree.comparator().compare(fromInRange, toInRange) > 0) {
            return null;
        }
        return map.subMap(fromInRange, fromInclusive, toInRange, toInclusive);
    }

    private void recursiveColumnRange(Map<NsdbColumnHandle, Domain> domainMap,
                                      NavigableMap<List<Comparable>, Set<PagePosition>> navigableMap,
                                      Map<Long, Set<Integer>> pagePositions,
                                      int column,
                                      List<Comparable> from,
                                      List<Comparable> to)
    {

        if (null == domainMap || domainMap.isEmpty()) {
            throw new SdbException(GENERIC_INTERNAL_ERROR, String.format("domain is null, domain: %s", domainMap));
        }

        if (null == navigableMap || navigableMap.isEmpty()) {
            return;
        }

        if (column >= columnHandles.size()) {
            addPositions(pagePositions, navigableMap.values());
            return;
        }

        NsdbColumnHandle nsdbColumnHandle = columnHandles.get(column);
        Domain domain = domainMap.get(nsdbColumnHandle);
        if (null == domain) {
            //联合索引情况下查询条件里只有前面几个字段，到这个字段就是最后一个了
            addPositions(pagePositions, navigableMap.values());
            return;
        }

        NavigableMap<List<Comparable>, Set<PagePosition>> subMap;

        List<Comparable> low = new ArrayList<>(from);
        List<Comparable> high = new ArrayList<>(to);
        low.add(null);
        high.add(null);

        if (domain.isOnlyNull()) {
            low.set(column, null);
            high.set(column, null);

            subMap = subMapInRange(navigableMap, low, true, high, true);

            recursiveColumnRange(domainMap, subMap, pagePositions, column + 1, low, high);
            return;
        }

        if (domain.isNullAllowed()) {
            low.set(column, null);
            high.set(column, null);

            subMap = subMapInRange(navigableMap, low, true, high, true);

            recursiveColumnRange(domainMap, subMap, pagePositions, column + 1, low, high);
        }

        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        for (int i = 0; i < ranges.size(); i++) {
            Range range = ranges.get(i);

            Marker rangeLow = range.getLow();
            Marker rangeHigh = range.getHigh();

            low.set(column, (Comparable) rangeLow.getValue());
            high.set(column, (Comparable) rangeHigh.getValue());

            subMap = subMapInRange(navigableMap,
                    low,
                    rangeLow.getBound() == Marker.Bound.EXACTLY,
                    high,
                    rangeHigh.getBound() == Marker.Bound.EXACTLY);

            recursiveColumnRange(domainMap, subMap, pagePositions, column + 1, low, high);
        }
    }

    private void addPositions(Map<Long, Set<Integer>> pagePositions, Collection<Set<PagePosition>> treeValues)
    {
        for (Set<PagePosition> positions : treeValues) {
            for (PagePosition position : positions) {
                long page = position.getPage();
                int positionInPage = position.getPosition();
                if (pagePositions.containsKey(page)) {
                    pagePositions.get(page).add(positionInPage);
                }
                else {
                    Set<Integer> positionsInPage = new HashSet<>(1);
                    positionsInPage.add(positionInPage);
                    pagePositions.put(page, positionsInPage);
                }
            }
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
