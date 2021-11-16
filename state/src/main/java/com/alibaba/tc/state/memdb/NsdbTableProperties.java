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

import com.alibaba.sdb.spi.session.PropertyMetadata;
import com.facebook.airlift.json.JsonCodec;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.sdb.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static java.util.Locale.ENGLISH;

public class NsdbTableProperties
{
    public static final String INDEXES = "indexes";
    public static final String ALLOW_DIRECTLY_RENAME_TO = "allow_directly_rename_to";

    private static final JsonCodec<List<Map<String, List<String>>>> listMapJsonCodec = listJsonCodec(mapJsonCodec(
            String.class, listJsonCodec(String.class)
    ));

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public NsdbTableProperties()
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(toListMap(
                        INDEXES,
                        "Indexes for each shard of the table"))
                .add(toBoolean(
                        ALLOW_DIRECTLY_RENAME_TO,
                        "whether allow force rename to this table(previous data in this table will be lost)"))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static List<Map<String, List<String>>> getIndexes(Map<String, Object> tableProperties)
    {
        List<Map<String, List<String>>> indexes = (List<Map<String, List<String>>>) tableProperties.get(INDEXES);
        return indexes == null ? new ArrayList<>() : indexes;
    }

    public static boolean allowForceRenameTo(Map<String, Object> tableProperties)
    {
        Boolean bl = (Boolean) tableProperties.get(ALLOW_DIRECTLY_RENAME_TO);
        return bl == null ? false : bl;
    }

    public static PropertyMetadata<List> toListMap(String name, String description)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                List.class,
                null,
                false,
                value -> listMapJsonCodec.fromJson(((String) value).toLowerCase(ENGLISH)),
                value -> value);
    }

    public static PropertyMetadata<Boolean> toBoolean(String name, String description)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                Boolean.class,
                null,
                false,
                value -> Boolean.parseBoolean(((String) value).toLowerCase(ENGLISH)),
                value -> value);
    }
}
