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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

public class NsdbConfig
{
    private int splitsPerWorder = Runtime.getRuntime().availableProcessors();
    private DataSize maxCachePerSplit = new DataSize(8, DataSize.Unit.GIGABYTE);

    private String nsdbDataDirectory = "file:///var/nsdb/data";

    public String getNsdbDataDirectory()
    {
        return nsdbDataDirectory;
    }

    @Deprecated
    public DataSize getMaxCachePerSplit()
    {
        return maxCachePerSplit;
    }

    public int getSplitsPerWorder()
    {
        return splitsPerWorder;
    }

    @Config("nsdb.data-directory")
    @ConfigDescription("Directory where schemas, tables, data, index are written")
    public NsdbConfig setNsdbDataDirectory(String nsdbDataDirectory)
    {
        this.nsdbDataDirectory = nsdbDataDirectory;
        return this;
    }

    @Deprecated
    @Config("nsdb.max-cache-per-split")
    public NsdbConfig setMaxCachePerSplit(DataSize maxCachePerSplit)
    {
        this.maxCachePerSplit = maxCachePerSplit;
        return this;
    }

    @Config("nsdb.splits-per-worker")
    public NsdbConfig setSplitsPerWorder(int splitsPerWorder)
    {
        this.splitsPerWorder = splitsPerWorder;
        return this;
    }
}
