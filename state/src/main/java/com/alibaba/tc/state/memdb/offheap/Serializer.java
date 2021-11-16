package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public interface Serializer<E> {
    @StaticMethod
    E deserialize(long addr);

    long allocAndSerialize(int extraSize);

    @StaticMethod
    void free(long addr, int extraSize);

    class Property {
        String name;
        Class clazz;
        long offsetOffheap;
        long offsetOnheap;
    }

    static long initProperties(Class clazz, Property[] properties)
    {
        Field[] fields = clazz.getDeclaredFields();

        int i = 0;
        long offset = 0;
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            Class type = field.getType();
            Property property = new Property();
            property.name = field.getName();
            property.clazz = type;
            property.offsetOffheap = offset;
            property.offsetOnheap = InternalUnsafe.objectFieldOffset(field);
            properties[i] = property;

            if (type == long.class) {
                offset += Long.BYTES;
            }
            else if (type == int.class) {
                offset += Integer.BYTES;
            }
            else if (type == short.class) {
                offset += Short.BYTES;
            }
            else if (type == boolean.class) {
                offset += Byte.BYTES;
            }
            else {
                throw new IllegalArgumentException("unsupported type");
            }

            i++;
        }

        return offset;
    }

    default void serialize(Property[] properties, long addr)
    {
        for (Property property : properties) {
            Class type = property.clazz;
            if (type == long.class) {
                InternalUnsafe.putLong(addr + property.offsetOffheap, InternalUnsafe.getLong(this, property.offsetOnheap));
            }
            else if (type == int.class) {
                InternalUnsafe.putInt(addr + property.offsetOffheap, InternalUnsafe.getInt(this, property.offsetOnheap));
            }
            else if (type == short.class) {
                InternalUnsafe.putShort(addr + property.offsetOffheap, InternalUnsafe.getShort(this, property.offsetOnheap));
            }
            else if (type == boolean.class) {
                InternalUnsafe.putByte(addr + property.offsetOffheap, InternalUnsafe.getBoolean(this, property.offsetOnheap) ? 1 : (byte) 0);
            }
            else {
                throw new IllegalArgumentException("unsupported type");
            }
        }
    }

    static <T extends Serializer<T>> T deserialize(Class<T> clazz, Property[] properties, long addr)
    {
        Object object = InternalUnsafe.allocateInstance(clazz);
        for (Property property : properties) {
            Class type = property.clazz;
            if (type == long.class) {
                InternalUnsafe.putLong(object, property.offsetOnheap, InternalUnsafe.getLong(addr + property.offsetOffheap));
            }
            else if (type == int.class) {
                InternalUnsafe.putInt(object, property.offsetOnheap, InternalUnsafe.getInt(addr + property.offsetOffheap));
            }
            else if (type == short.class) {
                InternalUnsafe.putShort(object, property.offsetOnheap, InternalUnsafe.getShort(addr + property.offsetOffheap));
            }
            else if (type == boolean.class) {
                InternalUnsafe.putBoolean(object, property.offsetOnheap, InternalUnsafe.getByte(addr + property.offsetOffheap) != 0);
            }
            else {
                throw new IllegalArgumentException("unsupported type");
            }
        }

        return (T) object;
    }
}
