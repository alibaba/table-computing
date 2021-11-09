package com.alibaba.tc.table;

import com.alibaba.tc.criteria.Criteria;
import com.alibaba.tc.criteria.JoinCriteria;
import com.alibaba.tc.exception.ColumnNameConflictException;
import com.alibaba.tc.exception.IllegalSizeException;
import com.alibaba.tc.exception.InconsistentColumnSizeException;
import com.alibaba.tc.function.AggregationFunction;
import com.alibaba.tc.function.OverWindowFunction;
import com.alibaba.tc.function.ScalarFunction;
import com.alibaba.tc.function.TransformFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import static com.alibaba.tc.offheap.InternalUnsafe.getInt;
import static com.alibaba.tc.offheap.InternalUnsafe.getLong;
import static com.alibaba.tc.offheap.InternalUnsafe.getObject;
import static com.alibaba.tc.offheap.InternalUnsafe.objectFieldOffset;
import static com.alibaba.tc.offheap.InternalUnsafe.putAddrAndLength;
import static com.alibaba.tc.offheap.InternalUnsafe.putInt;
import static com.alibaba.tc.offheap.InternalUnsafe.putLong;
import static com.alibaba.tc.offheap.InternalUnsafe.removeAddr;
import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

@NotThreadSafe
public class Table {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private final LinkedHashMap<String, Integer> columnName2Index = new LinkedHashMap<>();
    private final List<Column> columns;
    private int size;

    public void print() {
        System.out.println();
        System.out.println("Table@" + toHexString(hashCode()) + ":");
        System.out.println("________________________________________________________________________________________________________________________");
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < columns.size(); j++) {
                Column column = columns.get(j);
                System.out.println(column.name() + ": " + column.get(i));
            }
            System.out.println("------------------------------------------------------------------------------------------------------------------------");
            if (i >= 100 && i % 100 == 0) {
                Scanner scanner = new Scanner(System.in);
                if (scanner.hasNext()) {
                    continue;
                }
            }
        }
        System.out.println(format("total: %d rows", size));
        System.out.println();
    }

    public byte[] serialize() {
        List<Long> sizeList = new ArrayList<>(columns.size());
        long len = Integer.BYTES;
        for (Column column : columns) {
            len += Long.BYTES;
            long size = column.serializeSize();
            len += size;
            sizeList.add(size);
        }

        byte[] bytes = new byte[(int) len];

        long offset = ARRAY_BYTE_BASE_OFFSET;
        putInt(bytes, offset, columns.size());
        offset += Integer.BYTES;
        for (int i = 0; i < columns.size(); i++) {
            long size = sizeList.get(i);
            putLong(bytes, offset, size);
            offset += Long.BYTES;
            columns.get(i).serialize(bytes, offset - ARRAY_BYTE_BASE_OFFSET, size);
            offset += size;
        }
        return bytes;
    }

    private static long hbOffset;
    private static long addressOffset;
    private static long positionOffset;
    private static long limitOffset;
    private static Class directByteBufferClass;
    private static Class heapByteBufferClass;

    static {
        try {
            directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
            heapByteBufferClass = Class.forName("java.nio.HeapByteBuffer");
            Class clazz = Class.forName("java.nio.Buffer");
            addressOffset = objectFieldOffset(clazz.getDeclaredField("address"));
            positionOffset = objectFieldOffset(clazz.getDeclaredField("position"));
            limitOffset = objectFieldOffset(clazz.getDeclaredField("limit"));
            clazz = Class.forName("java.nio.ByteBuffer");
            hbOffset = objectFieldOffset(clazz.getDeclaredField("hb"));
        } catch (NoSuchFieldException e) {
            logger.error("", e);
        } catch (ClassNotFoundException e) {
            logger.error("", e);
        }
    }

    public static Table deserialize(ByteBuffer byteBuffer) {
        try {
            int position = getInt(byteBuffer, positionOffset);
            int limit = getInt(byteBuffer, limitOffset);
            long addr = getLong(byteBuffer, addressOffset);
            if (byteBuffer.getClass() == directByteBufferClass) {
                //避免InternalUnsafe在debug模式下进行内存边界检查时crash
                putAddrAndLength(addr + position, limit);
                Table table = deserialize(null, addr + position, limit);
                removeAddr(addr + position);
                return table;
            }
            if (byteBuffer.getClass() == heapByteBufferClass) {
                byte[] bytes = (byte[]) getObject(byteBuffer, hbOffset);
                if (null != bytes) {
                    return deserialize(bytes, ARRAY_BYTE_BASE_OFFSET + position, limit);
                }
            }
            throw new IllegalArgumentException(byteBuffer.getClass().getName());
        } finally {
        }
    }

    public static Table deserialize(byte[] bytes) {
        return deserialize(bytes, ARRAY_BYTE_BASE_OFFSET, bytes.length);
    }

    public static Table deserialize(byte[] bytes, long offset, int limit) {
        long start = offset;
        int columnsSize = getInt(bytes, offset);
        offset += Integer.BYTES;
        List<Column> columns = new ArrayList<>(columnsSize);
        for (int i = 0; i < columnsSize; i++) {
            long len = getLong(bytes, offset);
            offset += Long.BYTES;
            Column column = new Column();
            column.deserialize(bytes, offset, len);
            offset += len;
            columns.add(column);
        }
        if (offset - start != limit) {
            throw new IndexOutOfBoundsException();
        }
        return new Table(columns);
    }

    public static Table createEmptyTableLike(Table table) {
        List<Column> columns = new ArrayList<>(table.columns.size());
        for (int i = 0; i < table.columns.size(); i++) {
            columns.add(new Column(table.columns.get(i).name()));
        }

        return new Table(columns);
    }

    public Table(List<Column> columns) {
        this.columns = requireNonNull(columns);
        if (columns.size() < 1) {
            throw new IllegalArgumentException("should be at least one column for a table");
        }
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).name();
            if (columnName2Index.containsKey(name)) {
                throw new ColumnNameConflictException(name);
            }
            if (columns.get(i).size() != columns.get(0).size()) {
                throw new InconsistentColumnSizeException();
            }
            columnName2Index.put(name, i);
        }

        this.size = columns.get(0).size();
    }

    public void append(Table table, int row) {
        for (int i = 0; i < columns.size(); i++) {
            Column column = table.getColumn(i);
            if (column.getType() == Type.VARCHAR) {
                columns.get(i).addOffheap(column.getOffheap(row));
            } else {
                columns.get(i).add(column.get(row));
            }
        }
        this.size++;
    }

    public Table addColumns(List<Column> columns) {
        for (Column column : columns) {
            if (columnName2Index.containsKey(column.name())) {
                throw new ColumnNameConflictException(column.name());
            }
            if (column.size() != this.columns.get(0).size()) {
                throw new InconsistentColumnSizeException();
            }
            columnName2Index.put(column.name(), this.columns.size());
            this.columns.add(column);
        }
        return this;
    }

    public Integer getIndex(String columnName) {
        return columnName2Index.get(columnName);
    }

    public LinkedHashMap<String, Integer> getColumnIndex() {
        return columnName2Index;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public Column getColumn(String columnName) {
        return columns.get(getIndex(columnName));
    }

    public int size() {
        return size;
    }
    
    public boolean isEmpty() {
        return size <= 0;
    }

    public Index createIndex(String... columnNames) {
        if (null == columnNames || columnNames.length < 1) {
            throw new IllegalArgumentException("at least one column to create index");
        }
        Index index = new Index();
        for (int i = 0; i < size; i++) {
            List<Comparable> key = new ArrayList(columnNames.length);
            for (int j = 0; j < columnNames.length; j++) {
                String columnName = columnNames[j];
                Comparable k = columns.get(columnName2Index.get(columnName)).get(i);
                key.add(k);
            }
            index.put(key, i);
        }

        return index;
    }

    //todo: 通过DictionaryTable的方式只存行下标和原表的引用即可。这样可以提升性能但表的相关操作都要重新实现以匹配
    public Table filter(Criteria criteria) {
        List<Column> retColumns = new ArrayList<>(columns.size());
        for (Column column : columns) {
            retColumns.add(new Column(column.name()));
        }

        for (int i = 0; i < size; i++) {
            if (criteria.filter(new RowByTable(this, i))) {
                for (int j = 0; j < columns.size(); j++) {
                    retColumns.get(j).add(columns.get(j).get(i));
                }
            }
        }

        return new Table(retColumns);
    }

    private Comparable[] row(int row, int extra) {
        Comparable[] record = new Comparable[columns.size() + extra];
        for (int i = 0; i < this.columns.size(); i++) {
            record[i] = getColumn(i).get(row);
        }

        return record;
    }

    private List<Column> genColumns(String[] columnNames, boolean selectAll) {
        int newLength = selectAll ? this.columns.size() + columnNames.length : columnNames.length;
        List<Column> columns = new ArrayList<>(newLength);
        if (selectAll) {
            for (int i = 0; i < this.columns.size(); i++) {
                columns.add(new Column(this.columns.get(i).name()));
            }
        }
        for (int i = 0; i < columnNames.length; i++) {
            columns.add(new Column(columnNames[i]));
        }

        return columns;
    }

    /**
     *
     * @param scalarFunction        will pass every row to this function, returned array size must be equal to
     *                              additionalColumns.length, return null this row will be filtered
     * @param selectAll             whether select all columns
     * @param additionalColumns     if selectAll is true this is additional columns, else this will be all new columns
     * @return                      the new generated table
     */
    public Table select(ScalarFunction scalarFunction, boolean selectAll, String... additionalColumns) {
        List<Column> columns = genColumns(additionalColumns, selectAll);

        if (size < 1) {
            return new Table(columns);
        }

        int fieldsSize = additionalColumns.length;
        for (int i = 0; i < size; i++) {
            Comparable[] fields = scalarFunction.returnOneRow(new RowByTable(this, i));
            if (null == fields) {
                continue;
            }
            addRow(columns, fields, fieldsSize, selectAll, i);
        }

        return new Table(columns);
    }

    /**
     *
     * @param scalarFunction        will pass every row to this function, returned array size add transformFunction returned
     *                              element size in list must be equal to additionalColumns.length, return null this row
     *                              will be filtered
     * @param transformFunction     will pass every row to this function, returned element size in list add scalarFunction
     *                              returned array size must be equal to additionalColumns.length and all element size in
     *                              list must be equal. return null or empty list this row will be filtered
     * @param selectAll             whether select all columns
     * @param additionalColumns     if selectAll is true this is additional columns, else this will be all new columns
     * @return                      the new generated table
     */
    public Table select(ScalarFunction scalarFunction, TransformFunction transformFunction, boolean selectAll, String... additionalColumns) {
        List<Column> columns = genColumns(additionalColumns, selectAll);

        if (size < 1) {
            return new Table(columns);
        }

        int fieldsSize = additionalColumns.length;
        for (int i = 0; i < size; i++) {
            Row row = new RowByTable(this, i);
            Comparable[] oneRow = scalarFunction.returnOneRow(row);
            if (null == oneRow) {
                continue;
            }
            List<Comparable[]> rows = transformFunction.returnMultiRow(row);
            if (null == rows || rows.size() < 1) {
                continue;
            }
            Comparable[] wideRow = Arrays.copyOf(oneRow, oneRow.length + rows.get(0).length);
            for (Comparable[] fields : rows) {
                for (int k = 0; k < fields.length; k++) {
                    wideRow[oneRow.length + k] = fields[k];
                }
                addRow(columns, wideRow, fieldsSize, selectAll, i);
            }
        }

        return new Table(columns);
    }

    private void addRow(List<Column> columns, Comparable[] fields, int fieldsSize, boolean selectAll, int row) {
        if (fields.length != fieldsSize) {
            throw new IllegalSizeException("returned columns not equal to select columns (hint: if selectAll is true you only need return remain columns)");
        }

        if (selectAll) {
            Comparable[] record = row(row, fields.length);
            for (int i = 0; i < fieldsSize; i++) {
                record[this.columns.size() + i] = fields[i];
            }
            addRow(columns, record, 0);
        } else {
            addRow(columns, fields, 0);
        }
    }

    private List<Row> toRows(List<Integer> rows) {
        List<Row> ret = new ArrayList<>(rows.size());
        for (Integer row : rows) {
            ret.add(new RowByTable(this, row));
        }

        return ret;
    }

    static int addRow(List<Column> columns, List<Comparable> fields, int from) {
        int i = from;
        for (; i - from < fields.size(); i++) {
            columns.get(i).add(fields.get(i - from));
        }

        return i;
    }

    private int addRow(List<Column> columns, Comparable[] fields, int from) {
        int i = from;
        for (; i - from < fields.length; i++) {
            columns.get(i).add(fields[i - from]);
        }

        return i;
    }

    public static Table rowsToTable(List<Row> rows) {
        if (null == rows || rows.isEmpty()) {
            return null;
        }
        int columnSize = rows.get(0).size();
        int rowSize = rows.size();
        List<Column> columns = new ArrayList<>(columnSize);
        for (String name : rows.get(0).getColumnNames()) {
            columns.add(new Column(name, rowSize));
        }
        for (Row row : rows) {
            for (int i = 0; i < columnSize; i++) {
                columns.get(i).add(row.get(i));
            }
        }
        return new Table(columns);
    }

    public Table groupBy(Index existingIndex, AggregationFunction aggregationFunction, String[] groupByColumnNames, String... aggColumnNames) {
        List<Column> columns = new ArrayList<>(groupByColumnNames.length + aggColumnNames.length);
        for (int i = 0; i < groupByColumnNames.length; i++) {
            columns.add(new Column(groupByColumnNames[i]));
        }
        for (int i = 0; i < aggColumnNames.length; i++) {
            columns.add(new Column(aggColumnNames[i]));
        }

        if (size < 1) {
            return new Table(columns);
        }

        if (existingIndex == null) {
            existingIndex = createIndex(groupByColumnNames);
        }

        for (List<Comparable> key : existingIndex.getColumns2Rows().keySet()) {
            List<Integer> rows = existingIndex.getColumns2Rows().get(key);
            Comparable[] fields = aggregationFunction.agg(key, toRows(rows));
            if (null == fields) {
                continue;
            }

            int from = addRow(columns, key, 0);
            for (int j = 0; j < aggColumnNames.length; j++) {
                columns.get(from + j).add(fields[j]);
            }
        }

        return new Table(columns);
    }

    public Table over(Index existingIndex,
                      OverWindowFunction windowFunction,
                      String[] partitionByColumnNames,
                      String[] orderByColumnNames,
                      String... overColumnNames) {
        List<Column> columns = new ArrayList<>(overColumnNames.length);
        for (int i = 0; i < overColumnNames.length; i++) {
            columns.add(new Column(overColumnNames[i]));
        }

        if (existingIndex == null) {
            existingIndex = createIndex(partitionByColumnNames);
        }

        final Table that = this;
        for (List<Comparable> key : existingIndex.getColumns2Rows().keySet()) {
            List<Integer> rows = existingIndex.getColumns2Rows().get(key);
            rows.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    for (int i = 0; i < orderByColumnNames.length; i++) {
                        String name = orderByColumnNames[i];
                        Comparable c1 = that.getColumn(name).get(o1);
                        Comparable c2 = that.getColumn(name).get(o2);
                        if (null == c1 && null == c2) {
                            continue;
                        }
                        if (null == c1) {
                            return -1;
                        }
                        if (null == c2) {
                            return 1;
                        }
                        if (c1.equals(c2)) {
                            continue;
                        }
                        return c1.compareTo(c2);
                    }
                    return 0;
                }
            });
        }

        Table ret = createEmptyTableLike(this);
        for (List<Comparable> key : existingIndex.getColumns2Rows().keySet()) {
            List<Integer> rows = existingIndex.getColumns2Rows().get(key);
            List<Comparable[]> fieldsList = windowFunction.transform(key, toRows(rows));
            if (null == fieldsList) {
                continue;
            }
            if (fieldsList.size() != rows.size()) {
                throw new IllegalSizeException(format("rows: %d, returned: %d", rows.size(), fieldsList.size()));
            }

            for (Integer i : rows) {
                ret.append(this, i);
            }

            for (Comparable[] fields : fieldsList) {
                addRow(columns, fields, 0);
            }
        }

        return ret.addColumns(columns);
    }

    private int shallowCopyColumns(List<Column> retColumns, List<Column> columns, int from, Map<String, String> rename) {
        for (Column column : columns) {
            String columnName = rename != null ? rename.get(column.name()) : null;
            String newName = columnName == null ? column.name() : columnName;
            retColumns.add(new Column(newName));
        }

        return from;
    }

    private void addRow(List<Column> retColumns, List<Column> leftColumns, List<Column> rightColumns, int leftIndex, int rightIndex) {
        int k = 0;
        if (leftIndex >= 0 && rightIndex >= 0) {
            for (Column column : leftColumns) {
                retColumns.get(k++).add(column.get(leftIndex));
            }
            for (Column column : rightColumns) {
                retColumns.get(k++).add(column.get(rightIndex));
            }
        } else if (-1 == leftIndex && rightIndex >= 0) {
            for (int i = 0; i < leftColumns.size(); i++) {
                retColumns.get(k++).add(null);
            }
            for (Column column : rightColumns) {
                retColumns.get(k++).add(column.get(rightIndex));
            }
        } else if (-1 == rightIndex && leftIndex >= 0) {
            for (Column column : leftColumns) {
                retColumns.get(k++).add(column.get(leftIndex));
            }
            for (int i = 0; i < rightColumns.size(); i++) {
                retColumns.get(k++).add(null);
            }
        } else {
            throw new IllegalArgumentException(format("leftIndex: %d, rightIndex: %d", leftIndex, rightIndex));
        }
    }

    private Table internalJoin(Table right,
                               JoinCriteria criteria,
                               Map<String, String> leftRename,
                               Map<String, String> rightRename,
                               boolean appendNull,
                               boolean outerJoin) {
        List<Column> retColumns = new ArrayList<>(columns.size() + right.columns.size());
        int i = shallowCopyColumns(retColumns, columns, 0, leftRename);
        shallowCopyColumns(retColumns, right.columns, i, rightRename);

        Set<Integer> rightJoined = new HashSet<>();
        for (i = 0; i < size; i++) {
            List<Integer> rows = criteria.theOtherRows(new RowByTable(this, i));
            if (null != rows && !rows.isEmpty()) {
                for (Integer j : rows) {
                    addRow(retColumns, columns, right.columns, i, j);
                    if (outerJoin) {
                        rightJoined.add(j);
                    }
                }
            } else {
                if (appendNull) {
                    addRow(retColumns, columns, right.columns, i, -1);
                }
            }
        }
        if (outerJoin) {
            for (int j = 0; j < right.size; j++) {
                if (!rightJoined.contains(j)) {
                    addRow(retColumns, columns, right.columns, -1, j);
                }
            }
        }

        return new Table(retColumns);
    }

    public Table innerJoin(Table right, JoinCriteria criteria, Map<String, String> leftRename, Map<String, String> rightRename) {
        return internalJoin(right, criteria, leftRename, rightRename, false, false);
    }

    public Table join(Table right, JoinCriteria criteria, Map<String, String> leftRename, Map<String, String> rightRename) {
        return innerJoin(right, criteria, leftRename, rightRename);
    }

    public Table leftJoin(Table right, JoinCriteria criteria, Map<String, String> leftRename, Map<String, String> rightRename) {
        return internalJoin(right, criteria, leftRename, rightRename, true, false);
    }

    public Table outerJoin(Table right, JoinCriteria criteria, Map<String, String> leftRename, Map<String, String> rightRename) {
        return internalJoin(right, criteria, leftRename, rightRename, true, true);
    }

    /**
     * Notice: returned table compound with the references of this table's columns
     * @param columnNames   column names
     * @return table compound with the references of this table's columns denote by columnNames
     */
    public Table project(String... columnNames) {
        List<Column> columns = new ArrayList<>(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            columns.add(this.getColumn(columnNames[i]));
        }
        return new Table(columns);
    }

    /**
     * Notice: returned table compound with the references of this table's columns
     * @param columnNames   except column names
     * @return table compound with the references of this table's columns except the columns denote by columnNames
     */
    public Table projectNegative(String... columnNames) {
        List<Column> columns = new ArrayList<>(this.columns.size() - columnNames.length);
        for (Column column : this.columns) {
            int i = 0;
            for (; i < columnNames.length; i++) {
                if (column.name().equals(columnNames[i])) {
                    break;
                }
            }
            if (i == columnNames.length) {
                columns.add(column);
            }
        }

        return new Table(columns);
    }
}
