package com.alibaba.tc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class Threads {
    private Threads() {}

    public static ThreadFactory threadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setThreadFactory(new ClassLoaderThreadFactory(Thread.currentThread().getContextClassLoader(), defaultThreadFactory()))
                .build();
    }

    private static class ClassLoaderThreadFactory
            implements ThreadFactory
    {
        private final ClassLoader classLoader;
        private final ThreadFactory threadFactory;

        public ClassLoaderThreadFactory(ClassLoader classLoader, ThreadFactory threadFactory)
        {
            this.classLoader = classLoader;
            this.threadFactory = threadFactory;
        }

        @Override
        public Thread newThread(Runnable runnable)
        {
            Thread thread = threadFactory.newThread(runnable);
            thread.setContextClassLoader(classLoader);
            return thread;
        }
    }
}
