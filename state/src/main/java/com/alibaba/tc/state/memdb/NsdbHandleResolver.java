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
import com.alibaba.sdb.spi.ConnectorHandleResolver;
import com.alibaba.sdb.spi.ConnectorInsertTableHandle;
import com.alibaba.sdb.spi.ConnectorOutputTableHandle;
import com.alibaba.sdb.spi.ConnectorSplit;
import com.alibaba.sdb.spi.ConnectorTableHandle;
import com.alibaba.sdb.spi.ConnectorTableLayoutHandle;
import com.alibaba.sdb.spi.connector.ConnectorTransactionHandle;

public class NsdbHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return NsdbTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return NsdbTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return NsdbColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return NsdbSplit.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return NsdbTransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return NsdbOutputTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return NsdbInsertTableHandle.class;
    }
}
