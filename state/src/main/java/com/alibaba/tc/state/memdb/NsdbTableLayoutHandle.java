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

import com.alibaba.sdb.spi.ColumnHandle;
import com.alibaba.sdb.spi.ConnectorTableLayoutHandle;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class NsdbTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final NsdbTableHandle table;
    private final List<NsdbDataFragment> dataFragments;
    private final TupleDomain<ColumnHandle> constraint;
    private final Set<ColumnHandle> desiredColumns;

    @JsonCreator
    public NsdbTableLayoutHandle(
            @JsonProperty("table") NsdbTableHandle table,
            @JsonProperty("dataFragments") List<NsdbDataFragment> dataFragments,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constrait,
            @JsonProperty("desiredColumns") Set<ColumnHandle> desiredColumns)
    {
        this.table = requireNonNull(table, "table is null");
        this.dataFragments = requireNonNull(dataFragments, "dataFragments is null");
        this.constraint = requireNonNull(constrait, "constraint is null");
        this.desiredColumns = requireNonNull(desiredColumns, "desiredColumns is null");
    }

    @JsonProperty
    public NsdbTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<NsdbDataFragment> getDataFragments()
    {
        return dataFragments;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Set<ColumnHandle> getDesiredColumns()
    {
        return desiredColumns;
    }

    public String getConnectorId()
    {
        return table.getConnectorId();
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
