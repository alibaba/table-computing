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
import com.alibaba.sdb.spi.ConnectorPageSource;
import com.alibaba.sdb.spi.ConnectorSession;
import com.alibaba.sdb.spi.ConnectorSplit;
import com.alibaba.sdb.spi.ConnectorTableLayoutHandle;
import com.alibaba.sdb.spi.FixedPageSource;
import com.alibaba.sdb.spi.Page;
import com.alibaba.sdb.spi.SplitContext;
import com.alibaba.sdb.spi.connector.ConnectorPageSourceProvider;
import com.alibaba.sdb.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class NsdbPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final NsdbSplitPageStore partsPagesStore;

    @Inject
    public NsdbPageSourceProvider(NsdbPageStoreProvider nsdbPageStoreProvider)
    {
        requireNonNull(nsdbPageStoreProvider, "nsdbPageStoreProvider is null");
        this.partsPagesStore = requireNonNull(nsdbPageStoreProvider.getPartsPagesStore(), "nsdbPageStoreProvider.partsPagesStore is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        NsdbSplit nsdbSplit = (NsdbSplit) split;
        long tableId = nsdbSplit.getTableHandle().getTableId();
        int partNumber = nsdbSplit.getPartNumber();
        long expectedRows = nsdbSplit.getExpectedRows();

        List<Integer> columnIndexes = columns.stream()
                .map(NsdbColumnHandle.class::cast)
                .map(NsdbColumnHandle::getColumnIndex).collect(toList());
        List<Page> pages = partsPagesStore.getPages(
                tableId,
                partNumber,
                columnIndexes,
                expectedRows,
                nsdbSplit.getEffectivePredicate());

        return new FixedPageSource(pages);
    }
}
