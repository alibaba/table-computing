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
package com.alibaba.tc.state.memdb.insert;

import com.alibaba.sdb.metadata.Catalog;
import com.alibaba.sdb.metadata.CatalogManager;
import com.alibaba.sdb.spi.ColumnHandle;
import com.alibaba.sdb.spi.ConnectorId;
import com.alibaba.sdb.spi.ConnectorInsertTableHandle;
import com.alibaba.sdb.spi.ConnectorPageSink;
import com.alibaba.sdb.spi.ConnectorTableHandle;
import com.alibaba.sdb.spi.Page;
import com.alibaba.sdb.spi.PageSinkProperties;
import com.alibaba.sdb.spi.SchemaTableName;
import com.alibaba.sdb.spi.SdbException;
import com.alibaba.sdb.spi.block.Block;
import com.alibaba.sdb.spi.block.ByteArrayBlockOffheap;
import com.alibaba.sdb.spi.block.Int128ArrayBlockOffheap;
import com.alibaba.sdb.spi.block.IntArrayBlockOffheap;
import com.alibaba.sdb.spi.block.LongArrayBlockOffheap;
import com.alibaba.sdb.spi.block.ShortArrayBlockOffheap;
import com.alibaba.sdb.spi.block.VariableWidthBlockOffheap;
import com.alibaba.sdb.spi.connector.Connector;
import com.alibaba.sdb.spi.connector.ConnectorMetadata;
import com.alibaba.sdb.spi.connector.ConnectorPageSinkProvider;
import com.alibaba.sdb.spi.type.BigintType;
import com.alibaba.sdb.spi.type.BooleanType;
import com.alibaba.sdb.spi.type.DecimalType;
import com.alibaba.sdb.spi.type.Decimals;
import com.alibaba.sdb.spi.type.DoubleType;
import com.alibaba.sdb.spi.type.IntegerType;
import com.alibaba.sdb.spi.type.LongDecimalType;
import com.alibaba.sdb.spi.type.RealType;
import com.alibaba.sdb.spi.type.ShortDecimalType;
import com.alibaba.sdb.spi.type.SmallintType;
import com.alibaba.sdb.spi.type.TinyintType;
import com.alibaba.sdb.spi.type.Type;
import com.alibaba.sdb.spi.type.VarcharType;
import com.alibaba.sdb.sql.parser.ParsingException;
import com.alibaba.sdb.sql.tree.NodeLocation;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.alibaba.sdb.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DirectInsert
{
    private final CatalogManager catalogManager;
    private final String catalog;
    private final String schema;
    private final String table;
    private final String query;

    private Page page;
    private int rows;
    private int curColumn;

    private final List<? extends ColumnHandle> columnHandles;
    private final ConnectorMetadata connectorMetadata;
    private final ConnectorInsertTableHandle connectorInsertTableHandle;
    private final ConnectorPageSink connectorPageSink;

    private int line = 1;
    private int charPositionInLine = 1;

    private List<Object> params = new ArrayList<>();

    public DirectInsert(CatalogManager catalogManager,
                        String catalog,
                        String schema,
                        String table,
                        String query)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.query = requireNonNull(query, "query is null");

        Optional<Catalog> optionalCatalog = catalogManager.getCatalog(catalog);
        if (!optionalCatalog.isPresent()) {
            throw new IllegalArgumentException("unknown catalog: " + catalog);
        }
        ConnectorId connectorId = optionalCatalog.get().getConnectorId();
        Connector connector = optionalCatalog.get().getConnector(connectorId);
        connectorMetadata = connector.getMetadata(null);
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        ConnectorTableHandle connectorTableHandle = connectorMetadata.getTableHandle(null, schemaTableName);
        if (null == connectorTableHandle) {
            throw new SdbException(NOT_FOUND, format("table %s.%s.%s not exists", catalog, schema, table));
        }
        columnHandles = connectorTableHandle.getColumnHandles();

        ConnectorPageSinkProvider connectorPageSinkProvider = connector.getPageSinkProvider();
        connectorInsertTableHandle = connectorMetadata.beginInsert(null, connectorTableHandle);
        connectorPageSink = connectorPageSinkProvider.createPageSink(null, null, connectorInsertTableHandle, PageSinkProperties.defaultProperties());
    }

    public int insert()
    {
        return insertInternal();
    }

    public int insertInternal() throws ParsingException
    {
        parse();

        page = connectorPageSink.getPageForWriting();
        flush();
        if (page.getNewPositionCount() > 0) {
            connectorPageSink.appendPage(page);
        }

        CompletableFuture<Collection<Slice>> completableFuture = connectorPageSink.finish();
        Collection<Slice> sliceCollection;
        try {
            sliceCollection = completableFuture.get();
        }
        catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        catch (ExecutionException e) {
            throw new AssertionError(e);
        }
        connectorMetadata.finishInsert(null, connectorInsertTableHandle, sliceCollection, null);

        return rows;
    }

    private void writePage(String str, boolean isString)
    {
        Type type = columnHandles.get(curColumn).getColumnType();
        curColumn = (curColumn + 1) % columnHandles.size();
        String strTrim = str.trim();
        if (!isString && strTrim.equalsIgnoreCase("NULL")) {
            params.add(null);
            return;
        }

        Class clazz = type.getClass();
        try {
            Object value = null;
            if (clazz == BooleanType.class) {
                value = Boolean.parseBoolean(strTrim) ? 1 : (byte) 0;
            }
            else if (clazz == TinyintType.class) {
                value = Byte.parseByte(strTrim);
            }
            else if (clazz == SmallintType.class) {
                value = Short.parseShort(strTrim);
            }
            else if (clazz == RealType.class) {
                value = floatToIntBits(Float.parseFloat(strTrim));
            }
            else if (clazz == IntegerType.class) {
                value = Integer.parseInt(strTrim);
            }
            else if (clazz == DoubleType.class) {
                value = doubleToLongBits(Double.parseDouble(strTrim));
            }
            else if (clazz == ShortDecimalType.class) {
                BigDecimal bigDecimal = new BigDecimal(strTrim);
                value = Decimals.encodeShortScaledValue(bigDecimal, ((DecimalType) type).getScale());
            }
            else if (clazz == BigintType.class) {
                value = Long.parseLong(strTrim);
            }
            else if (clazz == LongDecimalType.class) {
                BigDecimal bigDecimal = new BigDecimal(strTrim);
                value = Decimals.encodeScaledValue(bigDecimal, ((DecimalType) type).getScale());
            }
            else if (clazz == VarcharType.class) {
                value = Slices.utf8Slice(str);
            }
            else {
                throw new IllegalArgumentException(format("unknown type: %s", type.getDisplayName()));
            }

            params.add(value);
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException(format(
                    "Parse to %s type error, parameter: '%s', cause: %s",
                    type.getDisplayName(),
                    str,
                    e.getMessage()));
        }
    }

    private void flush()
    {
        if (params.size() % columnHandles.size() != 0) {
            throw new IllegalArgumentException(format("params size cannot be divided by columns size, params.size: %d, columns size: %d", params.size(), columnHandles.size()));
        }
        curColumn = 0;
        for (Object param : params) {
            writeParam(param);
        }
    }

    private void writeParam(Object value)
    {
        if (page.isFull()) {
            connectorPageSink.appendPage(page);
            page = connectorPageSink.getPageForWriting();
        }

        Block block = page.getBlock(curColumn);
        Type type = columnHandles.get(curColumn).getColumnType();
        curColumn = (curColumn + 1) % columnHandles.size();
        if (curColumn == 0) {
            rows++;
            page.incrNewPositionCount();
        }

        if (value == null) {
            block.appendNullValue();
            return;
        }

        if (block.getClass() == ByteArrayBlockOffheap.class) {
            block.appendByte((byte) value);
        }
        else if (block.getClass() == ShortArrayBlockOffheap.class) {
            block.appendShort((short) value);
        }
        else if (block.getClass() == IntArrayBlockOffheap.class) {
            block.appendInt((int) value);
        }
        else if (block.getClass() == LongArrayBlockOffheap.class) {
            block.appendLong((long) value);
        }
        else if (block.getClass() == Int128ArrayBlockOffheap.class) {
            block.appendSlice((Slice) value);
        }
        else if (block.getClass() == VariableWidthBlockOffheap.class) {
            block.appendSlice((Slice) value);
        }
        else {
            throw new IllegalArgumentException(format("unknown block: %s, unknown type: %s", block.getClass().getName(), type.getDisplayName()));
        }
    }

    enum ParseExecuteState {
        PARAM,
        STRING,
        END
    }

    private void nextLine(char c)
    {
        if (c == '\n') {
            line++;
            charPositionInLine = 1;
        }
        else {
            charPositionInLine++;
        }
    }

    private int skipWhiteChar(String string, int start)
    {
        if (start >= string.length()) {
            return string.length();
        }

        while (start < string.length()) {
            char c = string.charAt(start);
            if (c > ' ') {
                break;
            }

            nextLine(c);

            start++;
        }
        return start;
    }

    @VisibleForTesting
    public int parse() throws ParsingException
    {
        ParseExecuteState state = ParseExecuteState.PARAM;
        StringBuilder sb = new StringBuilder();
        String param;
        line = 1;
        charPositionInLine = 1;

        int i = 0;
        while (i < query.length()) {
            char c = query.charAt(i);
            nextLine(c);

            if (c <= ' ') {
                if (state == ParseExecuteState.STRING) {
                    sb.append(c);
                }
                else if(state == ParseExecuteState.PARAM) {
                    i = skipWhiteChar(query, i);
                    if (',' != query.charAt(i)) {
                        throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                    }
                    continue;
                }
            }
            else if ('\'' == c) {
                if (state == ParseExecuteState.PARAM) {
                    state = ParseExecuteState.STRING;
                }
                else if (state == ParseExecuteState.STRING) {
                    i++;
                    if (i >= query.length()) {
                        param = sb.toString();
                        writePage(param, true);
                        sb.setLength(0);
                        state = ParseExecuteState.END;
                        break;
                    }
                    c = query.charAt(i);
                    if (c != '\'') {
                        i = skipWhiteChar(query, i);
                        c = query.charAt(i);
                        if (c != ',' && c != ';') {
                            throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                        }
                        else {
                            param = sb.toString();
                            writePage(param, true);
                            sb.setLength(0);
                            if (c == ';') {
                                state = ParseExecuteState.END;
                                break;
                            }
                            i = skipWhiteChar(query, i + 1);
                            if (i >= query.length()) {
                                throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                            }
                            state = ParseExecuteState.PARAM;
                            continue;
                        }
                    }
                    else {
                        sb.append(c);
                    }
                }
                else {
                    throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                }
            }
            else if (',' == c) {
                if (state == ParseExecuteState.STRING) {
                    sb.append(c);
                }
                else if (state == ParseExecuteState.PARAM) {
                    param = sb.toString();
                    writePage(param, false);
                    sb.setLength(0);
                    i = skipWhiteChar(query, i + 1);
                    if (i >= query.length()) {
                        throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                    }
                    continue;
                }
                else {
                    throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                }
            }
            else if (';' == c) {
                if (state == ParseExecuteState.STRING) {
                    sb.append(c);
                }
                else if (state == ParseExecuteState.PARAM) {
                    param = sb.toString();
                    writePage(param, false);
                    state = ParseExecuteState.END;
                    break;
                }
                else {
                    throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                }
            }
            else {
                if (state == ParseExecuteState.STRING || state == ParseExecuteState.PARAM) {
                    sb.append(c);
                }
                else {
                    throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
                }
            }

            i++;
        }

        if (state == ParseExecuteState.END) {
            String trim = query.substring(i).trim();
            if (!trim.isEmpty() && !trim.equals(";")) {
                throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
            }
        }
        else if (state == ParseExecuteState.PARAM) {
            param = sb.toString();
            if (param.trim().isEmpty()) {
                throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
            }
            writePage(param, false);
        }
        else {
            throw new ParsingException("parse error", new NodeLocation(line, charPositionInLine));
        }

        return query.length();
    }

    private void escape(StringBuilder sb, char c)
    {
        switch (c) {
            case 't':
                sb.append('\t');
                break;
            case 'b':
                sb.append('\b');
                break;
            case 'n':
                sb.append('\n');
                break;
            case 'r':
                sb.append('\r');
                break;
            case 'f':
                sb.append('\f');
                break;
            case '\'':
            case '"':
            case '\\':
                sb.append(c);
            default:
                throw new RuntimeException();
        }
    }
}
