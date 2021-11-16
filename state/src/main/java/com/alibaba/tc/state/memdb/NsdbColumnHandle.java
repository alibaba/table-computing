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
import com.alibaba.sdb.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class NsdbColumnHandle
        implements ColumnHandle
{
    //相同列名相同类型的列可能被 drop 后再 add 上，直接追加到 columnIndex 之后就好了，物理删除一列成本很高，让它随着行删除淡出就好了
    private final String columnName;
    private final Type columnType;
    private final int columnIndex;
    private boolean isDropped;

    @JsonCreator
    public NsdbColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("columnIndex") int columnIndex)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.columnIndex = columnIndex;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public int getColumnIndex()
    {
        return columnIndex;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    public static List<NsdbColumnHandle> extractColumnHandles(List<ColumnMetadata> columns)
    {
        List<NsdbColumnHandle> columnHandles = new ArrayList<>();
        int columnIndex = 0;
        for (ColumnMetadata column : columns) {
            columnHandles.add(new NsdbColumnHandle(column.getName(), column.getType(), columnIndex));
            columnIndex++;
        }
        return columnHandles;
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
        NsdbColumnHandle that = (NsdbColumnHandle) o;
        return Objects.equals(columnName, that.columnName)
                && Objects.equals(columnType, that.columnType)
                && Objects.equals(columnIndex, columnIndex);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, columnIndex);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("columnIndex", columnIndex)
                .toString();
    }
}
