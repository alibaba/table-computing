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
import com.alibaba.sdb.spi.ColumnMetadata;
import com.alibaba.sdb.spi.ConnectorTableHandle;
import com.alibaba.sdb.spi.ConnectorTableMetadata;
import com.alibaba.sdb.spi.SchemaTableName;
import com.alibaba.sdb.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class NsdbTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final Long tableId;
    private final List<NsdbColumnHandle> columnHandles;
    private final Map<String, ColumnHandle> mapColumnHandles = new HashMap<>();
    private final Map<String, Object> properties;

    public NsdbTableHandle(
            String connectorId,
            Long tableId,
            ConnectorTableMetadata tableMetadata)
    {
        this(connectorId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableId,
                NsdbColumnHandle.extractColumnHandles(tableMetadata.getColumns()),
                tableMetadata.getProperties());
    }

    @JsonCreator
    public NsdbTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") Long tableId,
            @JsonProperty("columnHandles") List<NsdbColumnHandle> columnHandles,
            @JsonProperty("properties") Map<String, Object> properties)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.properties = properties;

        synchronized (this) {
            this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

            for (NsdbColumnHandle columnHandle : columnHandles) {
                this.mapColumnHandles.put(columnHandle.getColumnName(), columnHandle);
            }
        }
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public Map<String, Object> getProperties()
    {
        return properties;
    }

    public Map<String, ColumnHandle> getMapColumnHandles()
    {
        return mapColumnHandles;
    }

    @Override
    @JsonProperty
    public List<NsdbColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    public Type getColumnType(int columnIndex)
    {
        NsdbColumnHandle columnHandle = getColumn(columnIndex);
        if (null == columnHandle) {
            return null;
        }

        return columnHandle.getColumnType();
    }

    public NsdbColumnHandle getColumnByName(String columnName)
    {
        return (NsdbColumnHandle) mapColumnHandles.get(columnName);
    }

    public NsdbColumnHandle getColumn(int columnIndex)
    {
        //Drop Column 只是标记删除 columnIndex 维持不变所以可以这么干
        return columnHandles.get(columnIndex);
    }

    public List<ColumnMetadata> getColumns()
    {
        return this.columnHandles.stream().map(
                (columnHandle) -> {
                    NsdbColumnHandle nsdbColumnHandle = (NsdbColumnHandle) columnHandle;
                    return nsdbColumnHandle.toColumnMetadata();
                }
        ).collect(toList());
    }

    public void addColumn(String columnName, Type columnType)
    {
        if (this.mapColumnHandles.containsKey(columnName)) {
            NsdbColumnHandle nsdbColumnHandle = (NsdbColumnHandle) mapColumnHandles.get(columnName);
            Type oldType = nsdbColumnHandle.getColumnType();
            if (!oldType.equals(columnType)) {
                throw new IllegalArgumentException(
                        Strings.lenientFormat("column %s already has type %s, cannot change type to %s",
                                columnName,
                                oldType.getDisplayName(),
                                columnType.getDisplayName()));
            }
            return;
        }
        NsdbColumnHandle columnHandle = new NsdbColumnHandle(columnName, columnType, this.columnHandles.size());
        synchronized (this) {
            if (this.mapColumnHandles.containsKey(columnName)) {
                return;
            }
            this.mapColumnHandles.put(columnName, columnHandle);
            this.columnHandles.add(columnHandle);
        }
    }

    public ConnectorTableMetadata toTableMetadata()
    {
        return new ConnectorTableMetadata(
                toSchemaTableName(),
                getColumns());
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getConnectorId(), getTableId());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NsdbTableHandle other = (NsdbTableHandle) obj;
        return Objects.equals(this.getConnectorId(), other.getConnectorId()) &&
                Objects.equals(this.getTableId(), other.getTableId());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("tableId", tableId)
                .add("columnHandles", columnHandles)
                .toString();
    }
}
