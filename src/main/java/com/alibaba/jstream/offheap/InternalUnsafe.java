package com.alibaba.jstream.offheap;

import com.alibaba.jstream.SystemProperty;
import com.alibaba.jstream.Threads;
import com.google.common.base.StandardSystemProperty;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import sun.misc.Unsafe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.String.format;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class InternalUnsafe {
    private static final Logger logger = LoggerFactory.getLogger(InternalUnsafe.class);
    private static final Object INTERNAL_UNSAFE;
    private static final Method ALLOCATE_ARRAY_METHOD;
    private static final Boolean UNALIGNED;
    private static final Unsafe unsafe;
    private static final int pageSize;
    private static final AtomicLong usedMemory = new AtomicLong();
    private static volatile long maxDirectMemory;
    private static volatile AtomicLong reservedMemory;
    private static final TreeMap<Long, Long> addr2length = new TreeMap<>();
    private static volatile long lastOutOfMemoryMillis = 0L;

    static {
        try {
            String maxMemory = "maxMemory";
            String reservedMemoryName = "reservedMemory";
            String javaVersion = StandardSystemProperty.JAVA_VERSION.value();
            if ("11".compareTo(javaVersion) <= 0) {
                maxMemory = "MAX_MEMORY";
                reservedMemoryName = "RESERVED_MEMORY";
            }
            Class clazz = Class.forName("java.nio.Bits");
            Field field = clazz.getDeclaredField(maxMemory);
            field.setAccessible(true);
            maxDirectMemory = field.getLong(null);

            field = clazz.getDeclaredField(reservedMemoryName);
            field.setAccessible(true);
            reservedMemory = (AtomicLong) field.get(null);

            new ScheduledThreadPoolExecutor(1, Threads.threadsNamed("active-gc")).scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        GlobalMemory globalMemory = new SystemInfo().getHardware().getMemory();
                        long totalPhysicalMemory = globalMemory.getTotal();
                        long availPhysicalMemory = globalMemory.getAvailable();
                        //可用内存不足20%更新最大内存为当前已使用内存量强制释放10%内存出来（参见NsdbPagesStore.expirePages）
                        //一段时间后maxDirectMemory将定格在物理内存的80%
                        if (availPhysicalMemory < totalPhysicalMemory * 2 / 10) {
                            if (getUsedMemory() + directMemoryUsed() < maxDirectMemory) {
//                                maxDirectMemory = getUsedMemory() + directMemoryUsed();
                            }
                        }
                        logger.info("maxDirectMemory: " + DataSize.ofBytes(maxDirectMemory()).to(GIGABYTE) +
                                ", usedMemory: " + DataSize.ofBytes(getUsedMemory()).to(GIGABYTE) +
                                ", bufferOffheap: " + DataSize.ofBytes(BufferOffheap.bufferOffheapSize()).to(GIGABYTE) +
                                ", directMemoryUsed: " + DataSize.ofBytes(directMemoryUsed()).to(GIGABYTE) +
                                ", totalPhysicalMemory: " + DataSize.ofBytes(totalPhysicalMemory).to(GIGABYTE) +
                                ", availPhysicalMemory: " + DataSize.ofBytes(availPhysicalMemory).to(GIGABYTE));
                        tryFullGC();
                    } catch (Throwable t) {
                        logger.error("", t);
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);
        } catch (IllegalAccessException e) {
            logger.error("", e);
        } catch (NoSuchFieldException e) {
            logger.error("", e);
        } catch (ClassNotFoundException e) {
            logger.error("", e);
        }
    }

    private static RuntimeException handleInaccessibleObjectException(RuntimeException e) {
        if ("java.lang.reflect.InaccessibleObjectException".equals(e.getClass().getName())) {
            return e;
        }
        throw e;
    }

    private static Throwable trySetAccessible(AccessibleObject object, boolean checkAccessible) {
        if (checkAccessible) {
            return new UnsupportedOperationException("Reflective setAccessible(true) disabled");
        }
        try {
            object.setAccessible(true);
            return null;
        } catch (SecurityException e) {
            return e;
        } catch (RuntimeException e) {
            return handleInaccessibleObjectException(e);
        }
    }

    private static ClassLoader getSystemClassLoader() {
        if (System.getSecurityManager() == null) {
            return ClassLoader.getSystemClassLoader();
        } else {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                @Override
                public ClassLoader run() {
                    return ClassLoader.getSystemClassLoader();
                }
            });
        }
    }

    private static ClassLoader getClassLoader(final Class<?> clazz) {
        if (System.getSecurityManager() == null) {
            return clazz.getClassLoader();
        } else {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                @Override
                public ClassLoader run() {
                    return clazz.getClassLoader();
                }
            });
        }
    }

    static {
        // attempt to access field Unsafe#theUnsafe
        final Object maybeUnsafe = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                    Throwable cause = trySetAccessible(unsafeField, false);
                    if (cause != null) {
                        return cause;
                    }
                    // the unsafe instance
                    return unsafeField.get(null);
                } catch (NoSuchFieldException e) {
                    return e;
                } catch (SecurityException e) {
                    return e;
                } catch (IllegalAccessException e) {
                    return e;
                } catch (NoClassDefFoundError e) {
                    // Also catch NoClassDefFoundError in case someone uses for example OSGI and it made
                    // Unsafe unloadable.
                    return e;
                }
            }
        });
        if (maybeUnsafe instanceof Throwable) {
            unsafe = null;
            logger.debug("sun.misc.Unsafe.theUnsafe: unavailable", (Throwable) maybeUnsafe);
        } else {
            unsafe = (Unsafe) maybeUnsafe;
            logger.debug("sun.misc.Unsafe.theUnsafe: available");
        }

        pageSize = unsafe.pageSize();

        final Boolean unaligned;
        Object maybeUnaligned = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Class<?> bitsClass = Class.forName("java.nio.Bits", false, getSystemClassLoader());
                    String fieldName = "UNALIGNED";
                    try {
                        Field unalignedField = bitsClass.getDeclaredField(fieldName);
                        if (unalignedField.getType() == boolean.class) {
                            long offset = unsafe.staticFieldOffset(unalignedField);
                            Object object = unsafe.staticFieldBase(unalignedField);
                            return unsafe.getBoolean(object, offset);
                        }
                    } catch (NoSuchFieldException ignore) {
                    }

                    Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
                    Throwable cause = trySetAccessible(unalignedMethod, true);
                    if (cause != null) {
                        return cause;
                    }
                    return unalignedMethod.invoke(null);
                } catch (NoSuchMethodException e) {
                    return e;
                } catch (SecurityException e) {
                    return e;
                } catch (IllegalAccessException e) {
                    return e;
                } catch (ClassNotFoundException e) {
                    return e;
                } catch (InvocationTargetException e) {
                    return e;
                }
            }
        });

        if (maybeUnaligned instanceof Boolean) {
            unaligned = (Boolean) maybeUnaligned;
            logger.debug("java.nio.Bits.unaligned: available " + unaligned);
        } else {
            unaligned = null;
            Throwable t = (Throwable) maybeUnaligned;
            logger.debug("java.nio.Bits.unaligned: unavailable, {}", t);
        }

        UNALIGNED = unaligned;
        logger.info("java.nio.Bits.unaligned: " + UNALIGNED);

        Object maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Class<?> internalUnsafeClass = getClassLoader(InternalUnsafe.class).
                            loadClass("jdk.internal.misc.Unsafe");
                    Method method = internalUnsafeClass.getDeclaredMethod("getUnsafe");
                    return method.invoke(null);
                } catch (Throwable e) {
                    return e;
                }
            }
        });
        Object internalUnsafe = null;
        Method m = null;
        if (!(maybeException instanceof Throwable)) {
            internalUnsafe = maybeException;
            final Object finalInternalUnsafe = internalUnsafe;
            maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        return finalInternalUnsafe.getClass().getDeclaredMethod(
                                "allocateUninitializedArray", Class.class, int.class);
                    } catch (NoSuchMethodException e) {
                        return e;
                    } catch (SecurityException e) {
                        return e;
                    }
                }
            });

            if (maybeException instanceof Method) {
                try {
                    m = (Method) maybeException;
                    byte[] bytes = (byte[]) m.invoke(internalUnsafe, byte.class, 8);
                    assert bytes.length == 8;
                } catch (IllegalAccessException e) {
                    maybeException = e;
                } catch (InvocationTargetException e) {
                    maybeException = e;
                }
            }
        }
        INTERNAL_UNSAFE = internalUnsafe;
        ALLOCATE_ARRAY_METHOD = m;
    }

    public static boolean[] copyOf(boolean[] original, int newLength) {
        boolean[] copy = (boolean[]) allocateUninitializedArray(boolean.class, newLength);
        System.arraycopy(original, 0, copy, 0,
                Math.min(original.length, newLength));
        return copy;
    }

    public static int[] copyOf(int[] original, int newLength) {
        int[] copy = (int[]) allocateUninitializedArray(int.class, newLength);
        System.arraycopy(original, 0, copy, 0,
                Math.min(original.length, newLength));
        return copy;
    }

    public static class Alloc {
        long addr;
        long realSize;

        public long getAddr() {
            return addr;
        }

        public int getRealSize() {
            return (int) realSize;
        }
    }

    public static Object allocateInstance(Class clazz) {
        try {
            return unsafe.allocateInstance(clazz);
        } catch (InstantiationException e) {
            logger.error("", e);
        }

        return null;
    }

    public static long directMemoryUsed() {
        return reservedMemory.get();
    }

    public static long maxDirectMemory() {
        return maxDirectMemory;
    }

    private synchronized static void tryFullGC() {
        if (usedMemory.get() + directMemoryUsed() >= maxDirectMemory * 0.85) {
            System.gc();
        }
        if (usedMemory.get() + directMemoryUsed() >= maxDirectMemory) {
            long now = System.currentTimeMillis();
            if (now - lastOutOfMemoryMillis > 5000) {
                logger.warn("triggered full gc by tryFullGC");
                fullGC();
                lastOutOfMemoryMillis = now;
            }
        }
    }

    private static final Object syncAlloc = new Object();

    public static long alloc(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }

        if (usedMemory.get() + directMemoryUsed() + size >= maxDirectMemory) {
            synchronized (syncAlloc) {
                if (usedMemory.get() + directMemoryUsed() + size >= maxDirectMemory) {
                    logger.warn("triggered full gc by alloc");
                    fullGC();

                    for (int i = 0; i < 60; i++) {
                        try {
                            Thread.sleep(1000);
                            if (usedMemory.get() + directMemoryUsed() + size < maxDirectMemory) {
                                //jmap -histo:live pid 之后Reference Handler和Finalizer线程执行完之后堆外内存才能被释放干净，使用 top -H -p pid 查看
                                Thread.sleep(10_000);
                                break;
                            }
                        } catch (InterruptedException e) {
                            logger.error("", e);
                        }
                    }
                    if (usedMemory.get() + directMemoryUsed() + size >= maxDirectMemory) {
                        logger.error("", new OutOfMemoryError());
                        throw new OutOfMemoryError();
                    }
                }
            }
        }

        long realSize = size + Long.BYTES;
        long addr = unsafe.allocateMemory(realSize);
        if (addr <= 0L) {
            logger.error("", new OutOfMemoryError());
            throw new OutOfMemoryError();
        }

        putAddrAndLength(addr, realSize);

        usedMemory.addAndGet(realSize);
        putLong(addr, realSize);

        return addr + Long.BYTES;
    }

    public static long getUsedMemory() {
        return usedMemory.get();
    }

    public static void removeAddr(long addr) {
        if (!SystemProperty.DEBUG) {
            return;
        }

        synchronized (addr2length) {
            addr2length.remove(addr);
        }
    }

    public static void putAddrAndLength(long addr, long length) {
        if (!SystemProperty.DEBUG) {
            return;
        }

        synchronized (addr2length) {
            addr2length.put(addr, length);
        }
    }

    private static void checkBound(long addr, long length) {
        if (!SystemProperty.DEBUG) {
            return;
        }

        synchronized (addr2length) {
            Long startAddr = addr2length.floorKey(addr);
            if (null == startAddr) {
                throw new IndexOutOfBoundsException();
            }

            Long correctLength = addr2length.get(startAddr);
            if (addr < startAddr || addr > (startAddr + correctLength)) {
                throw new IndexOutOfBoundsException();
            }

            if (addr + length > (startAddr + correctLength)) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    public static void free(long addr) {
        if (0L == addr) {
            return;
        }

        addr -= Long.BYTES;
        checkBound(addr, 0);
        removeAddr(addr);

        usedMemory.addAndGet(-unsafe.getLong(addr));
        unsafe.freeMemory(addr);
    }

    public static void setMemory(long addr, long size, byte value) {
        checkBound(addr, size);
        long bytesToSet = size - (size % 8);
        unsafe.setMemory(addr, bytesToSet, value);
        unsafe.setMemory(addr + bytesToSet, size - bytesToSet, value);
    }

    public static void copyMemory(long srcAddr, long destAddr, long length) {
        copyMemory(null, srcAddr, null, destAddr, length);
    }

    public static void copyMemory(Object src, long srcAddress, Object dest, long destAddress, long length) {
        if (null == src) {
            checkBound(srcAddress, length);
        }
        if (null == dest) {
            checkBound(destAddress, length);
        }
        long bytesToCopy = length - (length % 8);
        unsafe.copyMemory(src, srcAddress, dest, destAddress, bytesToCopy);
        unsafe.copyMemory(src, srcAddress + bytesToCopy, dest, destAddress + bytesToCopy, length - bytesToCopy);
    }

    public static void storeFence() {
        unsafe.storeFence();
    }

    public static void loadFence() {
        unsafe.loadFence();
    }

    public static void fullFence() {
        unsafe.fullFence();
    }

    public static int getIntVolatile(Object object, long addr) {
        return unsafe.getIntVolatile(object, addr);
    }

    public static int getByte(Object object, long addr) {
        return unsafe.getByte(object, addr);
    }

    public static byte getByte(long addr) {
        checkBound(addr, Byte.BYTES);
        return unsafe.getByte(addr);
    }

    public static void putByte(long addr, byte value) {
        checkBound(addr, Byte.BYTES);
        unsafe.putByte(addr, value);
    }

    public static short getShort(long addr) {
        checkBound(addr, Short.BYTES);
        return unsafe.getShort(addr);
    }

    public static void putShort(long addr, short value) {
        checkBound(addr, Short.BYTES);
        unsafe.putShort(addr, value);
    }

    public static int getInt(long addr) {
        checkBound(addr, Integer.BYTES);
        return unsafe.getInt(addr);
    }

    public static void putInt(long addr, int value) {
        checkBound(addr, Integer.BYTES);
        unsafe.putInt(addr, value);
    }

    public static long getLong(long addr) {
        checkBound(addr, Long.BYTES);
        return unsafe.getLong(addr);
    }

    public static void putLong(long addr, long value) {
        checkBound(addr, Long.BYTES);
        unsafe.putLong(addr, value);
    }

    public static double getDouble(long addr) {
        checkBound(addr, Double.BYTES);
        return unsafe.getDouble(addr);
    }

    public static void putDouble(long addr, double value) {
        checkBound(addr, Double.BYTES);
        unsafe.putDouble(addr, value);
    }

    public static int getIntFromSlice(Slice slice, int index) {
        return getInt(slice.getBase(), slice.getAddress() + index * ARRAY_INT_INDEX_SCALE);
    }

    public static boolean getBoolFromSlice(Slice slice, int index) {
        return getByte(slice.getBase(), slice.getAddress() + index * ARRAY_BYTE_INDEX_SCALE) != 0;
    }

    public static void setIntToSlice(Slice slice, int index, int value) {
        putInt(slice.getBase(), slice.getAddress() + index * ARRAY_INT_INDEX_SCALE, value);
    }

    public static void setBoolToSlice(Slice slice, int index, boolean value) {
        putBoolean(slice.getBase(), slice.getAddress() + index * ARRAY_BYTE_INDEX_SCALE, value);
    }

    public static long getAndAddLong(long addr, long value) {
        checkBound(addr, Long.BYTES);
        return unsafe.getAndAddLong(null, addr, value);
    }

    public static long objectFieldOffset(Field field) {
        return unsafe.objectFieldOffset(field);
    }

    public static Object getObject(Object object, long offset) {
        return unsafe.getObject(object, offset);
    }

    public static void putLong(Object object, long offset, long value) {
        unsafe.putLong(object, offset, value);
    }

    private static void checkBound(Object object, long offset, int unit) {
        if (null != object && object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            if (offset < ARRAY_BYTE_BASE_OFFSET || offset > ARRAY_BYTE_BASE_OFFSET + bytes.length - unit) {
                throw new IndexOutOfBoundsException(format("bytes.length: %d, offset: %d", bytes.length, offset));
            }
        }
    }

    public static long getLong(Object object, long offset) {
        checkBound(object, offset, Long.BYTES);
        return unsafe.getLong(object, offset);
    }

    public static void putInt(Object object, long offset, int value) {
        unsafe.putInt(object, offset, value);
    }

    public static int getInt(Object object, long offset) {
        checkBound(object, offset, Integer.BYTES);
        return unsafe.getInt(object, offset);
    }

    public static void putShort(Object object, long offset, short value) {
        unsafe.putShort(object, offset, value);
    }

    public static short getShort(Object object, long offset) {
        return unsafe.getShort(object, offset);
    }

    public static void putBoolean(Object object, long offset, boolean value) {
        unsafe.putBoolean(object, offset, value);
    }

    public static boolean getBoolean(Object object, long offset) {
        return unsafe.getBoolean(object, offset);
    }

    public static void putLong(Object object, String fieldName, long value) {
        try {
            long offset = unsafe.objectFieldOffset(object.getClass().getDeclaredField(fieldName));
            unsafe.putLong(object, offset, value);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("field not exists: %s, class: %s", fieldName, object.getClass().getName()));
        }
    }

    public static void putInt(Object object, String fieldName, int value) {
        try {
            long offset = unsafe.objectFieldOffset(object.getClass().getDeclaredField(fieldName));
            unsafe.putInt(object, offset, value);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("field not exists: %s, class: %s", fieldName, object.getClass().getName()));
        }
    }

    public static int getInt(Object object, String fieldName) {
        try {
            long offset = unsafe.objectFieldOffset(object.getClass().getDeclaredField(fieldName));
            return unsafe.getInt(object, offset);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("field not exists: %s, class: %s", fieldName, object.getClass().getName()));
        }
    }

    public static Object getObject(Object object, String fieldName) {
        try {
            long offset = unsafe.objectFieldOffset(object.getClass().getDeclaredField(fieldName));
            return unsafe.getObject(object, offset);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("field not exists: %s, class: %s", fieldName, object.getClass().getName()));
        }
    }

    public static int getInt(Class clazz, Object object, String fieldName) {
        try {
            long offset = unsafe.objectFieldOffset(clazz.getDeclaredField(fieldName));
            return unsafe.getInt(object, offset);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("field not exists: %s, class: %s", fieldName, clazz.getName()));
        }
    }

    public static long getLong(Class clazz, Object object, String fieldName) {
        try {
            long offset = unsafe.objectFieldOffset(clazz.getDeclaredField(fieldName));
            return unsafe.getLong(object, offset);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("field not exists: %s, class: %s", fieldName, clazz.getName()));
        }
    }

    public static void putObject(Object object, String fieldName, Object value) {
        try {
            long offset = unsafe.objectFieldOffset(object.getClass().getDeclaredField(fieldName));
            unsafe.putObject(object, offset, value);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("field not exists: %s, class: %s", fieldName, object.getClass().getName()));
        }
    }

    public static Object allocateUninitializedArray(Class<?> clazz, int size) {
        try {
            return ALLOCATE_ARRAY_METHOD.invoke(INTERNAL_UNSAFE, clazz, size);
        } catch (IllegalAccessException e) {
            throw new Error(e);
        } catch (InvocationTargetException e) {
            throw new Error(e);
        }
    }

    private static String getOutput(Process process) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String str;
        while ((str = bufferedReader.readLine()) != null) {
            System.out.println(str);
            sb.append(str).append("\n");
        }

        bufferedReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        while ((str = bufferedReader.readLine()) != null) {
            System.out.println(str);
            sb.append(str).append("\n");
        }

        return sb.toString();
    }

    public synchronized static void fullGC() {
        try {
            String javaVersion = StandardSystemProperty.JAVA_VERSION.value();
            String cmd = System.getProperty("java.home") + "/../bin/jmap -histo:live ";
            if (javaVersion.compareTo("11") >= 0) {
                //下面的full gc方法对 11.0.3.4-AJDK 有效，对mac jdk11.0.2无效
                cmd = System.getProperty("java.home") + "/bin/jmap -histo:live ";
            }

            String pid = ManagementFactory.getRuntimeMXBean().getName();
            int indexOf = pid.indexOf('@');
            if (indexOf > 0) {
                pid = pid.substring(0, indexOf);
            }

            cmd += pid;
            Runtime.getRuntime().exec("setsid -f " + cmd);
        } catch (Throwable t) {
            logger.error("", t);
            throw new RuntimeException(t);
        }
    }

    public static long indexMemorySize() {
        return getUsedMemory() - BufferOffheap.bufferOffheapSize();
    }
}
