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

import com.alibaba.sdb.spi.SdbException;
import com.alibaba.sdb.spi.function.Description;
import com.alibaba.sdb.spi.function.LiteralParameters;
import com.alibaba.sdb.spi.function.ScalarFunction;
import com.alibaba.sdb.spi.function.SqlType;
import com.alibaba.sdb.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.sdb.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.SliceUtf8.countCodePoints;

public final class NsdbFunctions
{
    private NsdbFunctions()
    {
    }

    private static long stringPositionFromStart(Slice string, Slice substring, long start, long instance)
    {
        if (start <= 0) {
            throw new SdbException(INVALID_FUNCTION_ARGUMENT, "'start' must be a positive number.");
        }
        if (instance <= 0) {
            throw new SdbException(INVALID_FUNCTION_ARGUMENT, "'instance' must be a positive number.");
        }
        if (substring.length() == 0) {
            return 1;
        }

        int foundInstances = 0;
        // set the initial index just before the start of the string
        // this is to allow for the initial index increment
        int index = (int) (start - 2);
        do {
            // step forwards through string
            index = string.indexOf(substring, index + 1);
            if (index < 0) {
                return 0;
            }
            foundInstances++;
        }
        while (foundInstances < instance);

        return countCodePoints(string, 0, index) + 1;
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring)
    {
        return stringPositionFromStart(string, substring, 1, 1);
    }

    @Description("returns index of first occurrence of a substring start from specific position (or 0 if not found)")
    @ScalarFunction("instr")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring, @SqlType(StandardTypes.BIGINT) long start)
    {
        return stringPositionFromStart(string, substring, start, 1);
    }

    @Description("returns index of n-th occurrence of a substring start from specific position (or 0 if not found)")
    @ScalarFunction("instr")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long instance)
    {
        return stringPositionFromStart(string, substring, start, instance);
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("locate")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringLocate(@SqlType("varchar(x)") Slice substring, @SqlType("varchar(y)") Slice string)
    {
        return stringPositionFromStart(string, substring, 1, 1);
    }

    @Description("returns index of first occurrence of a substring start from specific position (or 0 if not found)")
    @ScalarFunction("locate")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringLocate(@SqlType("varchar(x)") Slice substring, @SqlType("varchar(y)") Slice string, @SqlType(StandardTypes.BIGINT) long start)
    {
        return stringPositionFromStart(string, substring, start, 1);
    }

    @Description("returns index of n-th occurrence of a substring start from specific position (or 0 if not found)")
    @ScalarFunction("locate")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringLocate(@SqlType("varchar(x)") Slice substring, @SqlType("varchar(y)") Slice string, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long instance)
    {
        return stringPositionFromStart(string, substring, start, instance);
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("charindex")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long charindex(@SqlType("varchar(x)") Slice substring, @SqlType("varchar(y)") Slice string)
    {
        return stringPositionFromStart(string, substring, 1, 1);
    }

    @Description("returns distinct string number in x split by , ")
    @ScalarFunction("_distinct_count_str_")
    @LiteralParameters({"x"})
    @SqlType(StandardTypes.BIGINT)
    public static long distinctCountInString(@SqlType("varchar(x)") Slice string)
    {
        if (null == string) {
            return 0;
        }
        Set<String> distinctSet = new HashSet<>(Arrays.asList(string.toStringUtf8().split(",")));
        return distinctSet.size();
    }

    @Description("returns distinct string in x split by , ")
    @ScalarFunction("_distinct_str_")
    @LiteralParameters({"x"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice distinctString(@SqlType("varchar(x)") Slice string)
    {
        if (null == string) {
            return null;
        }
        Set<String> distinctSet = new HashSet<>(Arrays.asList(string.toStringUtf8().split(",")));
        return Slices.utf8Slice(String.join(",", distinctSet));
    }
}
