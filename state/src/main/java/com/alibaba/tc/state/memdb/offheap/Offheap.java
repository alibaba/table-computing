package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.type.LongDecimalType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public interface Offheap<T> extends Serializer<T>, ComparableOffheap<T>
{
    static Offheap offheaplize(Object object, Class type)
    {
        Class clazz = object.getClass();
        if (clazz == Slice.class && type == LongDecimalType.class) {
            return new LongDecimalOffheap((Slice) object);
        }
        if (clazz == Boolean.class) {
            return new BooleanOffheap(((Boolean) object).booleanValue());
        }
        if (clazz == Double.class) {
            return new DoubleOffheap(((Double) object).doubleValue());
        }
//        if (clazz == Integer.class) {
//            return new IntegerOffheap(((Integer) object).intValue());
//        }
        if (clazz == Long.class) {
            return new LongOffheap(((Long) object).longValue());
        }
        if (clazz == Slice.class) {
            Slice slice = (Slice) object;
            Slice slice1 = Slices.allocateDirect(slice.length());
            slice1.setBytes(0, slice);
            return new SliceOffheap(slice1);
        }

        throw new IllegalArgumentException("unsupported type");
    }
}
