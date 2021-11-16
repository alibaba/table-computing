package com.alibaba.tc.state.rocksdb;

import com.google.common.primitives.Longs;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Random;

import static java.lang.String.format;

public class Rocksdb {
    public static void main(String[] args) throws RocksDBException {
        RocksDB rocksDB = RocksDB.open("/tmp/rocksdb_test");

        long total = 10_000_000;
        long start = System.currentTimeMillis();
        for (long i = 0; i < total; i++) {
            byte[] key = Longs.toByteArray(new Random().nextInt((int) total));
            byte[] value = Longs.toByteArray(i);
            rocksDB.put(key, value);
        }
        long end = System.currentTimeMillis();
        System.out.println(format("tps: %f", 1000. * total / (end - start)));

        start = System.currentTimeMillis();
        for (long i = 0; i < total; i++) {
            byte[] key = Longs.toByteArray(new Random().nextInt((int) total));
            long value = Longs.fromByteArray(rocksDB.get(key));
            continue;
        }
        end = System.currentTimeMillis();
        System.out.println(format("qps: %f", 1000. * total / (end - start)));

        start = System.currentTimeMillis();
        for (long i = 0; i < total; i++) {
            long lFirst = new Random().nextInt((int) total - 11);
            byte[] first = Longs.toByteArray(lFirst);
            long lLast = lFirst + 10;
            RocksIterator rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(first);

            int j = 0;
            while (rocksIterator.isValid() && Longs.fromByteArray(rocksIterator.key()) < lLast && j < 10) {
                long value = Longs.fromByteArray(rocksDB.get(rocksIterator.key()));
                rocksIterator.next();
                j++;
            }
        }
        end = System.currentTimeMillis();
        System.out.println(format("qps: %f", 1000. * total / (end - start)));


    }
}
