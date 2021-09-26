package com.alibaba.jstream.sp;

import com.alibaba.jstream.sp.input.StreamTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StreamProcessing {
    private static final Logger logger = LoggerFactory.getLogger(StreamProcessing.class);

    protected final int thread;
    private final List<Thread> computeThreads;
    protected final StreamTable[] streamTables;
    //考虑到watermark和noDataDelay和网络延迟等情况在流表已经全部fetch完之后增加一个delay时间再返回true
    private final long finishDelay;
    private volatile long finishTime;
    private static Throwable globalException;
    private static final Set<StreamProcessing> allSP = new HashSet<>();

    public synchronized static void handleException(Throwable t) {
        logger.error("", t);
        globalException = t;
        for (StreamProcessing sp : allSP) {
            sp.stop();
        }
        allSP.clear();
    }

    public StreamProcessing(StreamTable... streamingTables) {
        this(Runtime.getRuntime().availableProcessors() * 2, streamingTables);
    }

    public StreamProcessing(int thread, StreamTable... streamTables) {
        this(thread, Duration.ofSeconds(33), streamTables);
    }

    public StreamProcessing(int thread, Duration finishDelay, StreamTable... streamTables) {
        this.thread = thread;
        computeThreads = new ArrayList<>(thread);
        this.streamTables = requireNonNull(streamTables);
        this.finishDelay = finishDelay.toMillis();
        allSP.add(this);
    }

    public Rehash rehash(String uniqueName, String... hashByColumnNames) {
        return new Rehash(thread, uniqueName, hashByColumnNames);
    }

    private boolean isFinished() {
        if (streamTables.length <= 0) {
            return false;
        }

        for (int i = 0; i < streamTables.length; i++) {
            if (!streamTables[i].isFinished()) {
                return false;
            }
        }

        long now = System.currentTimeMillis();
        if (0 == finishTime) {
            finishTime = now;
        }
        if (now - finishTime >= finishDelay) {
            return true;
        }
        return false;
    }

    public void compute(Compute compute) {
        for (int i = 0; i < streamTables.length; i++) {
            streamTables[i].start();
        }

        for (int i = 0; i < thread; i++) {
            int finalI = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (!isFinished() && !Thread.interrupted()) {
                            compute.compute(finalI);
                        }
                    } catch (InterruptedException e) {
                        logger.info("interrupted");
                    } catch (Throwable t) {
                        handleException(t);
                    }
                }
            }, "compute-" + i);
            computeThreads.add(t);
        }

        for (int i = 0; i < thread; i++) {
            computeThreads.get(i).start();
        }

        join();
        finishTime = 0;
    }

    public void stop() {
        for (int i = 0; i < thread; i++) {
            computeThreads.get(i).interrupt();
        }
    }

    private synchronized static void throwIf() {
        if (null != globalException) {
            throw new RuntimeException(globalException);
        }
    }

    private void join() {
        throwIf();
        for (int i = 0; i < thread; i++) {
            try {
                computeThreads.get(i).join();
            } catch (InterruptedException e) {
                //等待过程中被interrupt直接退出即可
                logger.info("interrupted");
            }
        }
        computeThreads.clear();
        throwIf();
    }
}
