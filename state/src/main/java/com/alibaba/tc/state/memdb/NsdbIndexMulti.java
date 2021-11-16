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

import com.alibaba.sdb.spi.predicate.Domain;
import com.alibaba.sdb.spi.predicate.Marker;
import com.alibaba.sdb.spi.predicate.Range;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.alibaba.sdb.spi.type.BooleanType;
import com.alibaba.sdb.spi.type.DecimalType;
import com.alibaba.sdb.spi.type.Type;
import com.alibaba.sdb.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

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

public class NsdbIndexMulti
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

    List<NsdbColumnHandle> columnHandles;
    TreeMap<List<Slice>, Set<PagePosition>> indexTree;

    public NsdbIndexMulti(List<NsdbColumnHandle> columnHandles)
    {
        for (NsdbColumnHandle nsdbColumnHandle : columnHandles) {
            if (nsdbColumnHandle.getColumnType().equals(BooleanType.BOOLEAN)) {
                throw new IllegalArgumentException("create index for boolean type is almost no use");
            }
        }

        this.columnHandles = columnHandles;
        this.indexTree = new TreeMap(new Comparator<List<Slice>>() {
            /**
             * 长度为0的Slice最小，null最大
             */
            @Override
            public int compare(List<Slice> e1,
                               List<Slice> e2)
            {
                List<Slice> columnValues1 = e1;
                List<Slice> columnValues2 = e2;
                for (int i = 0; i < columnValues1.size(); i++) {
                    Slice slice1 = columnValues1.get(i);
                    Slice slice2 = columnValues2.get(i);

                    Type columnType = columnHandles.get(i).getColumnType();
                    int cmp = compareSlice(slice1, slice2, columnType);

                    if (0 != cmp) {
                        return cmp;
                    }
                }
                return 0;
            }
        });
    }

    private int compareSlice(Slice slice1, Slice slice2, Type type)
    {
        if (slice1 == null && slice2 == null) {
            return 0;
        }
        if (slice1 == null && slice2 != null) {
            return 1;
        }
        if (slice1 != null && slice2 == null) {
            return -1;
        }
        if (slice1.length() <= 0 && slice2.length() > 0) {
            return -1;
        }
        if (slice2.length() <= 0 && slice1.length() > 0) {
            return 1;
        }
        if (type instanceof DecimalType) {
            return UnscaledDecimal128Arithmetic.compare(slice1, slice2);
        }
        else {
            return slice1.toStringUtf8().compareTo(slice2.toStringUtf8());
//            return slice1.compareTo(slice2);
        }
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
    public void insert1LevelIndex(List<Slice> key, PagePosition pagePosition)
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
    public void delete1LevelIndex(List<Slice> key, PagePosition pagePosition)
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
    public Map<Long, List<Integer>> use1LevelIndex(TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        Map<NsdbColumnHandle, Domain> domainMap = effectivePredicate.getDomains().get();
        List<Slice> low = new ArrayList<>();
        List<Slice> high = new ArrayList<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            low.add(Slices.allocate(0));
            high.add(null);
        }

        Map<Long, List<Integer>> pagePositions = new HashMap<>();
        recursiveColumnRange(domainMap, indexTree, pagePositions, 0, low, high);

        return pagePositions;
    }

    private void resetLow(List<Slice> slices, int start)
    {
        for (; start < slices.size(); start++) {
            slices.set(start, Slices.allocate(0));
        }
    }

    private void resetHigh(List<Slice> slices, int start)
    {
        for (; start < slices.size(); start++) {
            slices.set(start, null);
        }
    }

    private void recursiveColumnRange(Map<NsdbColumnHandle, Domain> domainMap,
                                      NavigableMap<List<Slice>, Set<PagePosition>> navigableMap,
                                      Map<Long, List<Integer>> pagePositions,
                                      int column,
                                      List<Slice> low,
                                      List<Slice> high)
    {
        if (null == domainMap) {
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

        NavigableMap<List<Slice>, Set<PagePosition>> subMap;

        if (domain.isOnlyNull()) {
            low.set(column, null);
            high.set(column, null);

            subMap = navigableMap.subMap(low, true, high, true);

            recursiveColumnRange(domainMap, subMap, pagePositions, column + 1, low, high);
            return;
        }

        if (domain.isNullAllowed()) {
            low.set(column, null);
            high.set(column, null);

            subMap = navigableMap.subMap(low, true, high, true);

            recursiveColumnRange(domainMap, subMap, pagePositions, column + 1, low, high);
        }

        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        for (int i = 0; i < ranges.size(); i++) {
            Range range = ranges.get(i);

            Marker rangeLow = range.getLow();
            Marker rangeHigh = range.getHigh();

            low.set(column, (Slice) rangeLow.getValue());
            high.set(column, (Slice) rangeHigh.getValue());

            resetLow(low, column + 1);
            resetHigh(high, column + 1);

            subMap = navigableMap.subMap(low,
                    rangeLow.getBound() == Marker.Bound.EXACTLY,
                    high,
                    rangeHigh.getBound() == Marker.Bound.EXACTLY);

            List<Slice> cur = null;
            for (List<Slice> key : subMap.navigableKeySet()) {
                if (null == cur) {
                    cur = key;
                }
                else if (compareSlice(key.get(column), cur.get(column), columnHandles.get(column).getColumnType()) == 0) {
                    continue;
                }
                else {
                    cur = key;
                }
                Slice slice = cur.get(column);
                ArrayList<Slice> newLow = new ArrayList<>(low);
                newLow.set(column, slice);
                ArrayList<Slice> newHigh = new ArrayList<>(high);
                newHigh.set(column, slice);
                recursiveColumnRange(domainMap, subMap, pagePositions, column + 1, newLow, newHigh);
            }
        }
    }

    private void addPositions(Map<Long, List<Integer>> pagePositions, Collection<Set<PagePosition>> treeValues)
    {
        for (Set<PagePosition> positions : treeValues) {
            for (PagePosition position : positions) {
                long page = position.getPage();
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
    public void insert1LevelIndexForFile(List<Slice> values, long position)
    {
    }

    /**
     * 插入2级索引
     * @param values 对应每个 columnHandles 的值
     * @param id Row id
     */
    public void insert2LevelIndex(List<Slice> values, long id)
    {
    }

    public boolean canUseIndex(TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        //只要联合索引的第一列能匹配上就可以用这个索引加速
        Optional<Map<NsdbColumnHandle, Domain>> domains = effectivePredicate.getDomains();
        if (null != columnHandles && domains.isPresent() && domains.get().containsKey(columnHandles.get(0))) {
            return true;
        }

        return false;
    }
}
