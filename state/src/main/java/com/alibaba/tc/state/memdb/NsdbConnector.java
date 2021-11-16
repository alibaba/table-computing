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

import com.alibaba.sdb.spi.connector.Connector;
import com.alibaba.sdb.spi.connector.ConnectorIndexProvider;
import com.alibaba.sdb.spi.connector.ConnectorMetadata;
import com.alibaba.sdb.spi.connector.ConnectorPageSinkProvider;
import com.alibaba.sdb.spi.connector.ConnectorPageSourceProvider;
import com.alibaba.sdb.spi.connector.ConnectorPlanOptimizerProvider;
import com.alibaba.sdb.spi.connector.ConnectorSplitManager;
import com.alibaba.sdb.spi.connector.ConnectorTransactionHandle;
import com.alibaba.sdb.spi.session.PropertyMetadata;
import com.alibaba.sdb.spi.transaction.IsolationLevel;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;

import static com.alibaba.sdb.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.alibaba.sdb.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class NsdbConnector
        implements Connector
{
    private static final Logger log = Logger.get(NsdbConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final NsdbMetadata metadata;
    private final NsdbSplitManager splitManager;
    private final NsdbPageSourceProvider pageSourceProvider;
    private final NsdbPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public NsdbConnector(
            LifeCycleManager lifeCycleManager,
            NsdbMetadata metadata,
            NsdbSplitManager splitManager,
            NsdbPageSourceProvider pageSourceProvider,
            NsdbPageSinkProvider pageSinkProvider,
            NsdbTableProperties tableProperties)
    {
        this.pageSinkProvider = pageSinkProvider;
        this.pageSourceProvider = pageSourceProvider;
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null").getTableProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return NsdbTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector nsdb");
        }
    }
}
