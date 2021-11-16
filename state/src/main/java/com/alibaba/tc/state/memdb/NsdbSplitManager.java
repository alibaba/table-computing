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

import com.alibaba.sdb.spi.ConnectorSession;
import com.alibaba.sdb.spi.ConnectorSplit;
import com.alibaba.sdb.spi.ConnectorSplitSource;
import com.alibaba.sdb.spi.ConnectorTableLayoutHandle;
import com.alibaba.sdb.spi.FixedSplitSource;
import com.alibaba.sdb.spi.connector.ConnectorSplitManager;
import com.alibaba.sdb.spi.connector.ConnectorTransactionHandle;
import com.alibaba.sdb.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

public class NsdbSplitManager
        implements ConnectorSplitManager
{
    private final int splitsPerWorker;

    @Inject
    public NsdbSplitManager(NsdbConfig config)
    {
        this.splitsPerWorker = config.getSplitsPerWorder();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        NsdbTableLayoutHandle layoutHandle = (NsdbTableLayoutHandle) layout;
        NsdbTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<NsdbColumnHandle> effectivePredicate = layoutHandle.getConstraint()
                .transform(NsdbColumnHandle.class::cast);

        List<NsdbDataFragment> dataFragments = layoutHandle.getDataFragments();

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (NsdbDataFragment dataFragment : dataFragments) {
            for (int i = 0; i < splitsPerWorker; i++) {
                splits.add(
                        new NsdbSplit(
                                dataFragment.getHostAddress(),
                                tableHandle,
                                i,
                                splitsPerWorker,
                                dataFragment.getRows(),
                                effectivePredicate));
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
