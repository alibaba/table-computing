package com.alibaba.jstream.table;

import com.alibaba.jstream.exception.InconsistentColumnTypeException;
import com.alibaba.jstream.exception.UnknownTypeException;
import com.alibaba.jstream.offheap.InternalUnsafe;
import com.alibaba.jstream.offheap.VarbyteBufferOffheap;

import java.io.UnsupportedEncodingException;

import static com.alibaba.jstream.offheap.InternalUnsafe.copyMemory;
import static com.alibaba.jstream.offheap.InternalUnsafe.getInt;
import static com.alibaba.jstream.offheap.InternalUnsafe.putInt;
import static com.alibaba.jstream.offheap.InternalUnsafe.putLong;
import static com.alibaba.jstream.ArrayUtil.DEFAULT_CAPACITY;
import static com.alibaba.jstream.util.ScalarUtil.toDouble;
import static com.alibaba.jstream.util.ScalarUtil.toInteger;
import static com.alibaba.jstream.util.ScalarUtil.toLong;
import static com.alibaba.jstream.util.ScalarUtil.toStr;
import static java.lang.String.format;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class Column<T extends Comparable> implements Serializable {
    private String name;
    private Type type;
    private int preNull;
    private int initSize;
    private ColumnInterface column;

    Column() {
    }

    public Column(String name) {
        this(name, DEFAULT_CAPACITY);
    }

    public Column(String name, int initSize) {
        this.name = name;
        this.initSize = initSize > 0 ? initSize : DEFAULT_CAPACITY;
    }

    public Column(String name, Type type) {
        this(name);
        initType(type);
    }

    private byte[] getNameBytes() {
        try {
            return name.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long serializeSize() {
        long len = Integer.BYTES + getNameBytes().length + Integer.BYTES + Integer.BYTES + Integer.BYTES;
        if (null != column) {
            len += Long.BYTES + column.serializeSize();
        }
        return len;
    }

    @Override
    public void serialize(byte[] bytes, long offset, long length) {
        long end = offset + length;
        offset += ARRAY_BYTE_BASE_OFFSET;

        long len = getNameBytes().length;
        putInt(bytes, offset, (int) len);
        offset += Integer.BYTES;
        copyMemory(getNameBytes(), ARRAY_BYTE_BASE_OFFSET, bytes, offset, len);
        offset += len;

        if (null == type) {
            putInt(bytes, offset, -1);
        } else {
            putInt(bytes, offset, type.ordinal());
        }
        offset += Integer.BYTES;

        putInt(bytes, offset, preNull);
        offset += Integer.BYTES;

        putInt(bytes, offset, initSize);
        if (null != column) {
            offset += Integer.BYTES;
            len = column.serializeSize();
            putLong(bytes, offset, len);
            offset += Long.BYTES;
            column.serialize(bytes, offset - ARRAY_BYTE_BASE_OFFSET, len);
            offset += len;
        }

        if (offset - ARRAY_BYTE_BASE_OFFSET != end) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public void deserialize(byte[] bytes, long offset, long length) {
        long end = offset + length;

        int nameLength = getInt(bytes, offset);
        offset += Integer.BYTES;

        try {
            byte[] nameBytes = new byte[nameLength];
            copyMemory(bytes, offset, nameBytes, ARRAY_BYTE_BASE_OFFSET, nameLength);
            name = new String(nameBytes, "UTF-8");
            offset += nameLength;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        int ordinal = getInt(bytes, offset);
        offset += Integer.BYTES;
        if (-1 == ordinal) {
            type = null;
        } else {
            type = Type.valueOf(ordinal);
        }

        preNull = getInt(bytes, offset);
        offset += Integer.BYTES;

        initSize = getInt(bytes, offset);
        offset += Integer.BYTES;

        if (offset < end) {
            switch (type) {
                case DOUBLE:
                    this.column = new DoubleColumn();
                    break;
                case BIGINT:
                    this.column = new LongColumn();
                    break;
                case VARCHAR:
                    this.column = new VarbyteColumn();
                    break;
                case INT:
                    this.column = new IntColumn();
                    break;
                default:
                    throw new UnknownTypeException(type.name());
            }
            long len = InternalUnsafe.getLong(bytes, offset);
            offset += Long.BYTES;
            column.deserialize(bytes, offset, len);
            offset += len;
        }
        if (offset != end) {
            throw new IndexOutOfBoundsException();
        }
    }

    private void initType(Type type) {
        this.type = type;
        switch (type) {
            case DOUBLE:
                this.column = new DoubleColumn(this.initSize);
                break;
            case BIGINT:
                this.column = new LongColumn(this.initSize);
                break;
            case VARCHAR:
                this.column = new VarbyteColumn(this.initSize);
                break;
            case INT:
                this.column = new IntColumn(this.initSize);
                break;
            default:
                throw new UnknownTypeException(type.name());
        }
        for (int i = 0; i < preNull; i++) {
            column.add(null);
        }
    }

    public void add(T value) {
        if (null != value) {
            if (null == type) {
                initType(Type.getType(value));
            } else {
                if (Type.getType(value) != type) {
                    throw new InconsistentColumnTypeException(format("%s %s", value.getClass().getName(), type.name()));
                }
            }

            column.add(value);
        } else {
            if (null == type) {
                preNull++;
            } else {
                column.add(value);
            }
        }
    }

    void addOffheap(VarbyteBufferOffheap.Offheap offheap) {
        if (null == column) {
            initType(Type.VARCHAR);
        }
        ((VarbyteColumn) column).addOffheap(offheap);
    }

    VarbyteBufferOffheap.Offheap getOffheap(int row) {
        if (row < preNull) {
            return null;
        }

        return ((VarbyteColumn) column).getOffheap(row);
    }

    public T get(int row) {
        if (row < preNull) {
            return null;
        }

        return (T) column.get(row);
    }

    public String getString(int row) {
        return toStr(get(row));
    }

    public Double getDouble(int row) {
        return toDouble(get(row));
    }

    public Long getLong(int row) {
        return toLong(get(row));
    }

    public Integer getInteger(int row) {
        return toInteger(get(row));
    }

    public String name() {
        return name;
    }

    public int size() {
        if (null == column) {
            return preNull;
        }

        return (int) column.size();
    }

    public Type getType() {
        return type;
    }
}
