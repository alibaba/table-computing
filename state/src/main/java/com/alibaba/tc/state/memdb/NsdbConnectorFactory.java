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

import com.alibaba.sdb.spi.ConnectorHandleResolver;
import com.alibaba.sdb.spi.connector.Connector;
import com.alibaba.sdb.spi.connector.ConnectorContext;
import com.alibaba.sdb.spi.connector.ConnectorFactory;
import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.google.inject.Injector;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class NsdbConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "nsdb";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new NsdbHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new NsdbModule(catalogName, context.getTypeManager(), context.getNodeManager()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(NsdbConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
