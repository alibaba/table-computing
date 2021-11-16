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

import com.alibaba.sdb.spi.ConnectorInsertTableHandle;
import com.alibaba.sdb.spi.ConnectorOutputTableHandle;
import com.alibaba.sdb.spi.ConnectorPageSink;
import com.alibaba.sdb.spi.ConnectorSession;
import com.alibaba.sdb.spi.HostAddress;
import com.alibaba.sdb.spi.NodeManager;
import com.alibaba.sdb.spi.PageSinkProperties;
import com.alibaba.sdb.spi.connector.ConnectorPageSinkProvider;
import com.alibaba.sdb.spi.connector.ConnectorTransactionHandle;
import com.google.common.annotations.VisibleForTesting;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NsdbPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final NsdbSplitPageStore partsPagesStore;
    private final HostAddress currentHostAddress;

    @Inject
    public NsdbPageSinkProvider(NsdbPageStoreProvider nsdbPageStoreProvider, NodeManager nodeManager)
    {
        this(nsdbPageStoreProvider.getPartsPagesStore(), requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort());
    }

    @VisibleForTesting
    public NsdbPageSinkProvider(NsdbSplitPageStore partsPagesStore, HostAddress currentHostAddress)
    {
        this.partsPagesStore = requireNonNull(partsPagesStore, "partsPagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkProperties pageSinkProperties)
    {
        checkArgument(!pageSinkProperties.isPartitionCommitRequired(), "Nsdb connector does not support partition commit");

        NsdbOutputTableHandle nsdbOutputTableHandle = (NsdbOutputTableHandle) outputTableHandle;
        NsdbTableHandle tableHandle = nsdbOutputTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(nsdbOutputTableHandle.getActiveTableIds().contains(tableId));

        return new NsdbPageSink(partsPagesStore, currentHostAddress, tableId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkProperties pageSinkProperties)
    {
        checkArgument(!pageSinkProperties.isPartitionCommitRequired(), "Nsdb connector does not support partition commit");

        NsdbInsertTableHandle memoryInsertTableHandle = (NsdbInsertTableHandle) insertTableHandle;
        NsdbTableHandle tableHandle = memoryInsertTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(memoryInsertTableHandle.getActiveTableIds().contains(tableId));

        return new NsdbPageSink(partsPagesStore, currentHostAddress, tableId);
    }
}
