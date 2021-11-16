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
import com.alibaba.sdb.spi.ConnectorId;
import com.alibaba.sdb.spi.ConnectorInsertTableHandle;
import com.alibaba.sdb.spi.ConnectorNewTableLayout;
import com.alibaba.sdb.spi.ConnectorOutputTableHandle;
import com.alibaba.sdb.spi.ConnectorSession;
import com.alibaba.sdb.spi.ConnectorTableHandle;
import com.alibaba.sdb.spi.ConnectorTableLayout;
import com.alibaba.sdb.spi.ConnectorTableLayoutHandle;
import com.alibaba.sdb.spi.ConnectorTableLayoutResult;
import com.alibaba.sdb.spi.ConnectorTableMetadata;
import com.alibaba.sdb.spi.ConnectorViewDefinition;
import com.alibaba.sdb.spi.Constraint;
import com.alibaba.sdb.spi.HostAddress;
import com.alibaba.sdb.spi.Node;
import com.alibaba.sdb.spi.NodeManager;
import com.alibaba.sdb.spi.SchemaNotFoundException;
import com.alibaba.sdb.spi.SchemaTableName;
import com.alibaba.sdb.spi.SchemaTablePrefix;
import com.alibaba.sdb.spi.SdbException;
import com.alibaba.sdb.spi.StandardErrorCode;
import com.alibaba.sdb.spi.ViewNotFoundException;
import com.alibaba.sdb.spi.connector.ConnectorMetadata;
import com.alibaba.sdb.spi.connector.ConnectorOutputMetadata;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.alibaba.sdb.spi.statistics.ComputedStatistics;
import com.alibaba.sdb.spi.statistics.TableStatistics;
import com.alibaba.sdb.spi.type.BigintType;
import com.alibaba.sdb.spi.type.BooleanType;
import com.alibaba.sdb.spi.type.DoubleType;
import com.alibaba.sdb.spi.type.IntegerType;
import com.alibaba.sdb.spi.type.LongDecimalType;
import com.alibaba.sdb.spi.type.RealType;
import com.alibaba.sdb.spi.type.ShortDecimalType;
import com.alibaba.sdb.spi.type.SmallintType;
import com.alibaba.sdb.spi.type.TinyintType;
import com.alibaba.sdb.spi.type.Type;
import com.alibaba.sdb.spi.type.VarcharType;
import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.sdb.plugin.NsdbTableProperties.getIndexes;
import static com.alibaba.sdb.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class NsdbMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(NsdbMetadata.class);
    public static final String SCHEMA_NAME = "default";

    private final NodeManager nodeManager;
    private final String connectorId;
    private final List<String> schemas = new ArrayList<>();
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<SchemaTableName, Long> tableIds = new ConcurrentHashMap<>();
    private final Map<Long, NsdbTableHandle> tables = new ConcurrentHashMap<>();
    private final Map<Long, Map<HostAddress, NsdbDataFragment>> tableDataFragments = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, String> views = new ConcurrentHashMap<>();
    private final NsdbSplitPageStore partsPagesStore;

    @Inject
    public NsdbMetadata(NodeManager nodeManager, ConnectorId connectorId, NsdbPageStoreProvider nsdbPageStoreProvider, NsdbConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.schemas.add(SCHEMA_NAME);

        requireNonNull(nsdbPageStoreProvider, "nsdbPageStoreProvider is null");
        nsdbPageStoreProvider.initialize(config, this);
        this.partsPagesStore = requireNonNull(nsdbPageStoreProvider.getPartsPagesStore(), "nsdbPageStoreProvider.partsPagesStore is null");
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(schemas);
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        if (schemas.contains(schemaName)) {
            throw new SdbException(ALREADY_EXISTS, format("Schema [%s] already exists", schemaName));
        }
        schemas.add(schemaName);
    }

    @Override
    public synchronized void dropSchema(ConnectorSession session, String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new SdbException(StandardErrorCode.NOT_FOUND, format("Schema [%s] does not exist", schemaName));
        }

        boolean tablesExist = tables.values().stream()
                .anyMatch(table -> table.getSchemaName().equals(schemaName));

        if (tablesExist) {
            throw new SdbException(StandardErrorCode.SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        verify(schemas.remove(schemaName));
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Long tableId = tableIds.get(schemaTableName);
        if (tableId == null) {
            return null;
        }
        return tables.get(tableId);
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        NsdbTableHandle nsdbTableHandle = (NsdbTableHandle) tableHandle;
        return nsdbTableHandle.toTableMetadata();
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return tables.values().stream()
                .filter(table -> schemaNameOrNull == null || table.getSchemaName().equals(schemaNameOrNull))
                .map(NsdbTableHandle::toSchemaTableName)
                .collect(toList());
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        NsdbTableHandle nsdbTableHandle = (NsdbTableHandle) tableHandle;
        return nsdbTableHandle.getMapColumnHandles();
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        NsdbColumnHandle nsdbColumnHandle = (NsdbColumnHandle) columnHandle;
        return nsdbColumnHandle.toColumnMetadata();
    }

    @Override
    public synchronized Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .collect(toMap(NsdbTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        NsdbTableHandle handle = (NsdbTableHandle) tableHandle;
        Long tableId = tableIds.remove(handle.toSchemaTableName());
        if (tableId != null) {
            tables.remove(tableId);
            tableDataFragments.remove(tableId);
            partsPagesStore.cleanUp(ImmutableSet.copyOf(tableIds.values()));
        }
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());

        NsdbTableHandle oldTableHandle = (NsdbTableHandle) tableHandle;
        Long oldTableId = oldTableHandle.getTableId();
        SchemaTableName oldTableName = oldTableHandle.toSchemaTableName();
        NsdbTableHandle newTableHandle = new NsdbTableHandle(
                oldTableHandle.getConnectorId(),
                newTableName.getSchemaName(),
                newTableName.getTableName(),
                oldTableId,
                oldTableHandle.getColumnHandles(),
                oldTableHandle.getProperties());

        if (tableIds.containsKey(newTableName)) {
            Long newTableId = tableIds.get(newTableName);
            tableIds.put(newTableName, oldTableId);
            tableIds.remove(oldTableName);
            tables.remove(newTableId);
            tables.put(oldTableId, newTableHandle);

            tableDataFragments.remove(newTableId);
            partsPagesStore.removeTableId(newTableId);
            partsPagesStore.resetTableId(oldTableId, newTableHandle);
            return;
        }

        checkTableNotExists(newTableName);

        tableIds.put(newTableName, oldTableId);
        tableIds.remove(oldTableName);
        tables.put(oldTableId, newTableHandle);
        partsPagesStore.resetTableId(oldTableId, newTableHandle);
    }

    private void verifyType(Type type)
    {
        if (!(type.getClass() == VarcharType.class) &&
                !(type.getClass() == BigintType.class) &&
                !(type.getClass() == IntegerType.class) &&
                !(type.getClass() == SmallintType.class) &&
                !(type.getClass() == TinyintType.class) &&
                !(type.getClass() == DoubleType.class) &&
                !(type.getClass() == RealType.class) &&
                !(type.getClass() == BooleanType.class) &&
                !(type.getClass() == LongDecimalType.class) &&
                !(type.getClass() == ShortDecimalType.class)) {
            throw new IllegalArgumentException(String.format("have not support %s type, you can use 'varchar bigint int smallint tinyint double real boolean decimal' type up to now", type.getDisplayName()));
        }
    }

    private void verifyColumnType(ConnectorTableMetadata tableMetadata)
    {
        List<ColumnMetadata> columnMetadataList = tableMetadata.getColumns();
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            verifyType(columnMetadata.getType());
        }
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        verifyColumnType(tableMetadata);
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized NsdbOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        log.info("tableName: %s", tableMetadata.getTable().getTableName());

        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Nsdb nodes available");

        long nextId = nextTableId.getAndIncrement();
        NsdbTableHandle table = new NsdbTableHandle(
                connectorId,
                nextId,
                tableMetadata);

        List<List<NsdbColumnHandle>> indexesColumns = new ArrayList<>();
        List<Map<String, List<String>>> indexes = getIndexes(tableMetadata.getProperties());
        for (Map<String, List<String>> index : indexes) {
            for (String indexName : index.keySet()) {
                List<String> indexColumns = index.get(indexName);
                List<NsdbColumnHandle> columnHandles = new ArrayList<>();
                for (int i = 0; i < indexColumns.size(); i++) {
                    String columnName = indexColumns.get(i);
                    NsdbColumnHandle nsdbColumnHandle = table.getColumnByName(columnName);
                    if (null == nsdbColumnHandle) {
                        throw new NullPointerException(format("cannot create index, %s column is not exists", columnName));
                    }
                    columnHandles.add(nsdbColumnHandle);
                }
                indexesColumns.add(columnHandles);

            }
        }

        tableIds.put(tableMetadata.getTable(), nextId);
        tables.put(table.getTableId(), table);
        tableDataFragments.put(table.getTableId(), new HashMap<>());

        partsPagesStore.initialize(table.getTableId(), table);
        for (List<NsdbColumnHandle> columnHandles : indexesColumns) {
            partsPagesStore.createIndex(table.getTableId(), columnHandles);
        }

        return new NsdbOutputTableHandle(table, ImmutableSet.copyOf(tableIds.values()));
    }

    private void checkSchemaExists(String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    private void checkTableNotExists(SchemaTableName tableName)
    {
        if (tables.values().stream()
                .map(NsdbTableHandle::toSchemaTableName)
                .anyMatch(tableName::equals)) {
            throw new SdbException(ALREADY_EXISTS, format("Table [%s] already exists", tableName.toString()));
        }
        if (views.keySet().contains(tableName)) {
            throw new SdbException(ALREADY_EXISTS, format("View [%s] already exists", tableName.toString()));
        }
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        NsdbOutputTableHandle nsdbOutputHandle = (NsdbOutputTableHandle) tableHandle;

        updateRowsOnHosts(nsdbOutputHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized NsdbInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        NsdbTableHandle nsdbTableHandle = (NsdbTableHandle) tableHandle;
        return new NsdbInsertTableHandle(nsdbTableHandle, ImmutableSet.copyOf(tableIds.values()));
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        NsdbInsertTableHandle nsdbInsertHandle = (NsdbInsertTableHandle) insertHandle;

        updateRowsOnHosts(nsdbInsertHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        requireNonNull(viewMetadata, "viewMetadata is null");
        SchemaTableName viewName = viewMetadata.getTable();
        requireNonNull(viewName, "viewName is null");
        checkSchemaExists(viewName.getSchemaName());
        if (getTableHandle(session, viewName) != null) {
            throw new SdbException(ALREADY_EXISTS, "Table already exists: " + viewName);
        }

        if (replace) {
            views.put(viewName, viewData);
        }
        else if (views.putIfAbsent(viewName, viewData) != null) {
            throw new SdbException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
    }

    @Override
    public synchronized List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return views.keySet().stream()
                .filter(viewName -> (schemaNameOrNull == null) || schemaNameOrNull.equals(viewName.getSchemaName()))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return views.entrySet().stream()
                .filter(entry -> prefix.matches(entry.getKey()))
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> new ConnectorViewDefinition(entry.getKey(), Optional.empty(), entry.getValue())));
    }

    private void updateRowsOnHosts(NsdbTableHandle table, Collection<Slice> fragments)
    {
        checkState(
                tableDataFragments.containsKey(table.getTableId()),
                "Uninitialized table [%s.%s]",
                table.getSchemaName(),
                table.getTableName());
        Map<HostAddress, NsdbDataFragment> dataFragments = tableDataFragments.get(table.getTableId());

        for (Slice fragment : fragments) {
            NsdbDataFragment nsdbDataFragment = NsdbDataFragment.fromSlice(fragment);
            dataFragments.merge(nsdbDataFragment.getHostAddress(), nsdbDataFragment, NsdbDataFragment::merge);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        return TableStatistics.empty();
    }

    @Override
    public synchronized List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        requireNonNull(handle, "handle is null");
        checkArgument(handle instanceof NsdbTableHandle);
        NsdbTableHandle nsdbTableHandle = (NsdbTableHandle) handle;
        checkState(
                tableDataFragments.containsKey(nsdbTableHandle.getTableId()),
                "Inconsistent state for the table [%s.%s]",
                nsdbTableHandle.getSchemaName(),
                nsdbTableHandle.getTableName());

        List<NsdbDataFragment> expectedFragments = ImmutableList.copyOf(
                tableDataFragments.get(nsdbTableHandle.getTableId()).values());

        NsdbTableLayoutHandle layoutHandle = new NsdbTableLayoutHandle(nsdbTableHandle, expectedFragments, constraint.getSummary(), desiredColumns.orElse(ImmutableSet.of()));
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, layoutHandle), constraint.getSummary()));
    }

    @Override
    public synchronized ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(
                handle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }
//
//    public void addColumn(long tableId, String columnName, Type columnType)
//    {
//        tables.get(tableId).addColumn(columnName, columnType);
//    }
}
