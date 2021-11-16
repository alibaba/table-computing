package com.alibaba.tc.state.memdb.benchmark;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Threads {
    private Threads() {
    }

    public static ThreadFactory threadsNamed(String nameFormat) {
        return (new ThreadFactoryBuilder()).setNameFormat(nameFormat).setThreadFactory(new ContextClassLoaderThreadFactory(Thread.currentThread().getContextClassLoader(), Executors.defaultThreadFactory())).build();
    }

    public static ThreadFactory daemonThreadsNamed(String nameFormat) {
        return (new ThreadFactoryBuilder()).setNameFormat(nameFormat).setDaemon(true).setThreadFactory(new ContextClassLoaderThreadFactory(Thread.currentThread().getContextClassLoader(), Executors.defaultThreadFactory())).build();
    }

    private static class ContextClassLoaderThreadFactory implements ThreadFactory {
        private final ClassLoader classLoader;
        private final ThreadFactory delegate;

        public ContextClassLoaderThreadFactory(ClassLoader classLoader, ThreadFactory delegate) {
            this.classLoader = classLoader;
            this.delegate = delegate;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = this.delegate.newThread(runnable);
            thread.setContextClassLoader(this.classLoader);
            return thread;
        }
    }
}
