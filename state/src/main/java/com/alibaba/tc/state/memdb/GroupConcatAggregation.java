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

import com.alibaba.sdb.spi.block.BlockBuilder;
import com.alibaba.sdb.spi.function.AggregationFunction;
import com.alibaba.sdb.spi.function.CombineFunction;
import com.alibaba.sdb.spi.function.InputFunction;
import com.alibaba.sdb.spi.function.OutputFunction;
import com.alibaba.sdb.spi.function.SqlType;
import com.alibaba.sdb.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.alibaba.sdb.spi.type.VarcharType.VARCHAR;

@AggregationFunction("group_concat")
public final class GroupConcatAggregation
{
    private GroupConcatAggregation() {}

    @InputFunction
    public static void input(SliceState state, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        String dst = "";
        if (null != value) {
            dst = value.toStringUtf8();
        }
        if (null != state && state.getSlice() != null && state.getSlice().length() > 0) {
            dst = state.getSlice().toStringUtf8() + "," + value.toStringUtf8();
        }
        state.setSlice(Slices.wrappedBuffer(dst.getBytes()));
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState)
    {
        String dst = "";
        if (null != state && state.getSlice() != null && state.getSlice().length() > 0) {
            dst = state.getSlice().toStringUtf8();
        }
        if (null != otherState && otherState.getSlice() != null && otherState.getSlice().length() > 0) {
            if (dst.length() > 0) {
                dst = dst + "," + otherState.getSlice().toStringUtf8();
            }
            else {
                dst = otherState.getSlice().toStringUtf8();
            }
        }
        state.setSlice(Slices.wrappedBuffer(dst.getBytes()));
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(SliceState state, BlockBuilder out)
    {
        if (state.getSlice() == null) {
            out.appendNull();
        }
        else {
            VARCHAR.writeSlice(out, state.getSlice());
        }
    }
}
