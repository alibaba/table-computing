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

import com.alibaba.sdb.spi.ConnectorSplit;
import com.alibaba.sdb.spi.HostAddress;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.alibaba.sdb.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class NsdbSplit
        implements ConnectorSplit
{
    private final HostAddress address;
    private final NsdbTableHandle tableHandle;
    private final int totalPartsPerWorker; // how many concurrent reads there will be from one worker
    private final int partNumber; // part of the pages on one worker that this splits is responsible
    private final long expectedRows;
    private final TupleDomain<NsdbColumnHandle> effectivePredicate;

    @JsonCreator
    public NsdbSplit(
            @JsonProperty("address") HostAddress address,
            @JsonProperty("tableHandle") NsdbTableHandle tableHandle,
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalPartsPerWorker") int totalPartsPerWorker,
            @JsonProperty("expectedRows") long expectedRows,
            @JsonProperty("effectivePredicate") TupleDomain<NsdbColumnHandle> effectivePredicate)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partNumber = partNumber;
        this.totalPartsPerWorker = totalPartsPerWorker;
        this.address = requireNonNull(address, "address is null");
        this.expectedRows = expectedRows;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @JsonProperty
    public NsdbTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public int getTotalPartsPerWorker()
    {
        return totalPartsPerWorker;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @JsonProperty
    public TupleDomain<NsdbColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NodeSelectionStrategy.HARD_AFFINITY;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return sortedCandidates;
    }

    @JsonProperty
    public long getExpectedRows()
    {
        return expectedRows;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("address", address)
                .add("tableHandle", tableHandle)
                .add("partNumber", partNumber)
                .add("totalPartsPerWorker", totalPartsPerWorker)
                .add("expectedRows", expectedRows)
                .add("effectivePredicate", effectivePredicate)
                .toString();
    }
}
