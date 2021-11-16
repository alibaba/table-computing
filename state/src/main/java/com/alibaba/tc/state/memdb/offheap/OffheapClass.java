package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;
import com.google.common.base.Throwables;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class OffheapClass {
    private static final AtomicInteger idGenerator = new AtomicInteger(0);
    private static final Map<Short, Object> id2Object = new HashMap<>();
    private static final Map<Short, Class> id2Class = new HashMap<>();
    private static final Map<Class, Short> class2Id = new HashMap<>();

    public static short putClass(Class clazz)
    {
        Short id = class2Id.get(clazz);
        if (null != id) {
            return id;
        }

        synchronized (idGenerator) {
            int i = idGenerator.getAndIncrement();
            if (i > Short.MAX_VALUE) {
                throw new RuntimeException("off-heap class id: " + i + " overflow, which > Short.MAX_VALUE: " + Short.MAX_VALUE);
            }

            id = (short) i;

            try {
                id2Object.put(id, InternalUnsafe.allocateInstance(clazz));
            }
            catch (Throwable t) {
                throw new RuntimeException(Throwables.getStackTraceAsString(t));
            }

            id2Class.put(id, clazz);
            class2Id.put(clazz, id);
        }

        return id;
    }

    public static Object getObjectById(short id)
    {
        assert null != id2Object.get(id);
        return id2Object.get(id);
    }
}
