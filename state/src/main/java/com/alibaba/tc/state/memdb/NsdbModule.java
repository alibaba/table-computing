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

import com.alibaba.sdb.spi.ConnectorId;
import com.alibaba.sdb.spi.NodeManager;
import com.alibaba.sdb.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class NsdbModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;
    private final NodeManager nodeManager;

    public NsdbModule(String connectorId, TypeManager typeManager, NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(NodeManager.class).toInstance(nodeManager);

        binder.bind(NsdbConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorId.class).toInstance(new ConnectorId(connectorId));
        binder.bind(NsdbMetadata.class).in(Scopes.SINGLETON);
        binder.bind(NsdbSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(NsdbPageStoreProvider.class).in(Scopes.SINGLETON);
        binder.bind(NsdbPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(NsdbPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(NsdbTableProperties.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(NsdbConfig.class);
    }
}
