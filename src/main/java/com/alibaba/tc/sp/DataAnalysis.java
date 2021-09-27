package com.alibaba.tc.sp;

import com.alibaba.tc.sp.input.StreamTable;
import com.alibaba.tc.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.tc.table.Table.createEmptyTableLike;

public class DataAnalysis extends StreamProcessing {
    private static final Logger logger = LoggerFactory.getLogger(DataAnalysis.class);

    public DataAnalysis(StreamTable streamingTable) {
        this(Runtime.getRuntime().availableProcessors() * 2, streamingTable);
    }

    public DataAnalysis(int thread, StreamTable streamTable) {
        super(thread, Duration.ofSeconds(0), streamTable);
    }

    public List<Table>[] rehashAllData(String uniqueName, String... hashByColumnNames) {
        Rehash rehash = rehash(uniqueName, hashByColumnNames);
        List<Table>[] ret = new List[thread];
        for (int i = 0; i < thread; i++) {
            ret[i] = new ArrayList<>();
        }
        compute(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                Table table = streamTables[0].consume();
                List<Table> tables = rehash.rehash(table, myThreadIndex);
                ret[myThreadIndex].addAll(tables);
            }
        });
        rehash.waitOtherServers();

        //wait期间来自其它server的table
        for (int i = 0; i < thread; i++) {
            ret[i].addAll(rehash.tablesInThread(i));
        }

        rehash.close();
        return ret;
    }

    public Table mergeToOneTable(List<Table> tables) {
        if (null == tables || tables.isEmpty()) {
            return null;
        }
        Table table = createEmptyTableLike(tables.get(0));
        for (Table tmp : tables) {
            for (int i = 0; i < tmp.size(); i++) {
                table.append(tmp, i);
            }
        }
        return table;
    }
}
