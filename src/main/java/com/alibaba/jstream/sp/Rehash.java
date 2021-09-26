package com.alibaba.jstream.sp;

import com.alibaba.jstream.network.client.Client;
import com.alibaba.jstream.network.server.Server;
import com.alibaba.jstream.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.jstream.SystemProperty.getMyHash;
import static com.alibaba.jstream.SystemProperty.getNodeByHash;
import static com.alibaba.jstream.SystemProperty.getSelf;
import static com.alibaba.jstream.SystemProperty.getServerCount;
import static com.alibaba.jstream.Threads.threadsNamed;
import static com.alibaba.jstream.network.Command.REHASH_FINISHED;
import static com.alibaba.jstream.table.Table.createEmptyTableLike;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class Rehash {
    private static final Logger logger = LoggerFactory.getLogger(Rehash.class);

    private static final Map<String, Rehash> rehashes = new ConcurrentHashMap<>();
    private final int thread;
    private final String uniqueName;
    private final int myHash;
    private final int serverCount;
    private final String[] hashByColumnNames;
    private final List<Table>[] tablesInThread;
    private final Object[] locks;
    private final Server server;
    private final Client[][] clients;
    private final Table[][][] tablesInServer;
    private final int batchSize = 40000;
    private final long flushInterval = 1000;
    private volatile long lastFlushTime;
    private final Duration requestTimeout = Duration.ofSeconds(30);
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private final boolean[] finished;

    /**
     * java -jar jstream_task.jar -Dself=localhost:8888 -Dall=localhost:8888,127.0.0.1:9999
     *
     * @param uniqueName must be globally unique. we need use this name to decide rehash data from other servers
     *                   should be processed by which Rehash object.
     *                   automatically generate this name may lead to subtle race condition problem in concurrent case,
     *                   "name order" on different server may be different so let user define this name may be more sensible.
     */
    Rehash(int thread, String uniqueName, String... hashByColumnNames) {
        this.thread = thread;
        this.uniqueName = requireNonNull(uniqueName);
        this.hashByColumnNames = requireNonNull(hashByColumnNames);
        if (hashByColumnNames.length < 1) {
            throw new IllegalArgumentException();
        }

        this.myHash = getMyHash();
        this.serverCount = getServerCount();
        if (null != getSelf()) {
            Node self = getSelf();
            server = new Server(false, self.getHost(), self.getPort(), 1, 2);
            startServer();
            clients = new Client[serverCount][thread];
            for (int i = 0; i < serverCount; i++) {
                if (i == myHash) {
                    continue;
                }
                Node node = getNodeByHash(i);
                for (int j = 0; j < thread; j++) {
                    clients[i][j] = newClient(node.getHost(), node.getPort());
                }
            }
        } else {
            server = null;
            clients = new Client[0][0];
        }
        finished = new boolean[serverCount];
        tablesInServer = new Table[thread][serverCount][thread];

        for (int i = 0; i < thread; i++) {
            tablesInServer[i] = new Table[serverCount][thread];
            for (int j = 0; j < serverCount; j++) {
                tablesInServer[i][j] = new Table[thread];
            }
        }

        locks = new Object[thread];
        tablesInThread = new ArrayList[thread];
        for (int i = 0; i < thread; i++) {
            locks[i] = new Object();
            tablesInThread[i] = new ArrayList<>();
        }

        rehashes.put(uniqueName, this);
    }

    public void close() {
        server.close();
        for (int i = 0; i < serverCount; i++) {
            if (i == myHash) {
                continue;
            }
            for (int j = 0; j < thread; j++) {
                clients[i][j].close();
            }
        }
        rehashes.remove(uniqueName);
    }

    public void waitOtherServers() {
        if (serverCount <= 1) {
            return;
        }
        finished[myHash] = true;
        for (int i = 0; i < serverCount; i++) {
            if (i == myHash) {
                continue;
            }
            try {
                clients[i][0].request(REHASH_FINISHED, uniqueName, myHash);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        while (true) {
            try {
                reentrantLock.lock();
                boolean flag = true;
                for (int i = 0; i < serverCount; i++) {
                    if (!finished[i]) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    break;
                }
                try {
                    long nanos = condition.awaitNanos(requestTimeout.toNanos());
                    if (nanos <= 0) {
                        throw new RuntimeException("wait timeout");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                reentrantLock.unlock();
            }
        }
    }

    public static int otherServerFinished(String uniqueName, int serverHash) {
        Rehash rehash = rehashes.get(uniqueName);
        rehash.finished(serverHash);
        return 0;
    }

    private void finished(int serverHash) {
        try {
            reentrantLock.lock();
            finished[serverHash] = true;
            condition.signal();
        } finally {
            reentrantLock.unlock();
        }
    }

    private void startServer() {
        newSingleThreadExecutor(threadsNamed("server")).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start();
                } catch (CertificateException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (SSLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private Client newClient(String host, int port) {
        for (int i = 0; i < 600; i++) {
            try {
                return new Client(false, host, port, requestTimeout);
            } catch (Throwable t) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.info("interrupted");
                }
            }
        }
        throw new RuntimeException(format("cannot create client to host: %s, port: %d", host, port));
    }

    public static int fromOtherServer(String uniqueName, int thread, ByteBuffer data) {
        Rehash rehash = rehashes.get(uniqueName);
        Table table = Table.deserialize(data);
        synchronized (rehash.locks[thread]) {
            rehash.tablesInThread[thread].add(table);
        }
        return table.size();
    }

    private void toAnotherServer(Table table, int row, int hash, int myThreadIndex) {
        int i = hash % serverCount;
        int j = hash % thread;
        if (null == tablesInServer[myThreadIndex][i][j]) {
            tablesInServer[myThreadIndex][i][j] = createEmptyTableLike(table);
        }
        tablesInServer[myThreadIndex][i][j].append(table, row);
        flushBySize(i, j, myThreadIndex);
    }

    private void flushByInterval(int myThreadIndex) {
        long now = System.currentTimeMillis();
        if (now - lastFlushTime >= flushInterval) {
            for (int i = 0; i < serverCount; i++) {
                if (i == myHash) {
                    continue;
                }
                for (int j = 0; j < thread; j++) {
                    request(i, j, myThreadIndex);
                }
            }
            lastFlushTime = now;
        }
    }

    private int requestWithRetry(int server, int myThreadIndex, Table table, int toThread) {
        int i = 0;
        int retryTimes = 3;
        while (true) {
            try {
                return clients[server][myThreadIndex].request("rehash",
                        uniqueName,
                        toThread,
                        table);
            } catch (Throwable t) {
                i++;
                logger.error("request error {} times", i, t);
                if (i >= retryTimes) {
                    return -1;
                }
                try {
                    Thread.sleep(5000);
                    Node node = getNodeByHash(server);
                    clients[server][myThreadIndex].close();
                    clients[server][myThreadIndex] = newClient(node.getHost(), node.getPort());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void request(int server, int toThread, int myThreadIndex) {
        Table table = tablesInServer[myThreadIndex][server][toThread];
        if (null != table && table.size() > 0) {
            int ret = requestWithRetry(server, myThreadIndex, table, toThread);
            if (ret != table.size()) {
                throw new IllegalStateException(format("the peer received size: %d not equal to table.size: %d", ret, table.size()));
            }
            tablesInServer[myThreadIndex][server][toThread] = null;
        }
    }

    private void flushBySize(int serverHash, int toThread, int myThreadIndex) {
        if (tablesInServer[myThreadIndex][serverHash][toThread].size() >= batchSize) {
            request(serverHash, toThread, myThreadIndex);
        }
    }

    public List<Table> rebalance(Table table, int myThreadIndex) {
        return rehash(table, myThreadIndex, false);
    }

    public List<Table> rehash(Table table, int myThreadIndex) {
        return rehash(table, myThreadIndex, true);
    }

    private List<Table> rehash(Table table, int myThreadIndex, boolean isHash) {
        //长时间没有数据的情况下也可以被触发，参见AbstractStreamTable.consume
        flushByInterval(myThreadIndex);

        Table[] tables = new Table[thread];
        for (int i = 0; i < thread; i++) {
            tables[i] = createEmptyTableLike(table);
        }

        Random random = null;
        if (!isHash) {
            random = new Random();
        }
        for (int i = 0; i < table.size(); i++) {
            int h;
            if (isHash) {
                List<Comparable> key = new ArrayList<>(hashByColumnNames.length);
                for (int j = 0; j < hashByColumnNames.length; j++) {
                    key.add(table.getColumn(hashByColumnNames[j]).get(i));
                }
                h = abs(key.hashCode());
            } else {
                h = random.nextInt(serverCount * thread);
            }
            if (h % serverCount != myHash) {
                toAnotherServer(table, i, h, myThreadIndex);
                continue;
            }
            h %= thread;
            tables[h].append(table, i);
        }

        for (int i = 0; i < thread; i++) {
            if (i == myThreadIndex) {
                continue;
            }
            synchronized (locks[i]) {
                tablesInThread[i].add(tables[i]);
            }
        }

        List<Table> ret = tablesInThread(myThreadIndex);
        ret.add(tables[myThreadIndex]);
        return ret;
    }

    List<Table> tablesInThread(int threadIndex) {
        List<Table> ret;
        synchronized (locks[threadIndex]) {
            ret = tablesInThread[threadIndex];
            tablesInThread[threadIndex] = new ArrayList<>();
        }
        return ret;
    }
}
