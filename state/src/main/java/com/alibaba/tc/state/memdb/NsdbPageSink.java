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

import com.alibaba.sdb.spi.ConnectorPageSink;
import com.alibaba.sdb.spi.HostAddress;
import com.alibaba.sdb.spi.Page;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class NsdbPageSink
        implements ConnectorPageSink
{
    private final NsdbSplitPageStore partsPagesStore;
    private final HostAddress currentHostAddress;
    private final long tableId;
    private long addedRows;

    public NsdbPageSink(NsdbSplitPageStore partsPagesStore, HostAddress currentHostAddress, long tableId)
    {
        this.partsPagesStore = requireNonNull(partsPagesStore, "partsPagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
        this.tableId = tableId;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        partsPagesStore.add(tableId, page);

        if (page.getNewPositionCount() == 0) {
            addedRows += page.getPositionCount();
        }
        else {
            addedRows += page.getNewPositionCount();
        }

        return NOT_BLOCKED;
    }

    @Override
    public Page getPageForWriting() {
        return partsPagesStore.getPageForWriting(tableId);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of(new NsdbDataFragment(currentHostAddress, addedRows).toSlice()));
    }

    @Override
    public void abort()
    {
    }
}
