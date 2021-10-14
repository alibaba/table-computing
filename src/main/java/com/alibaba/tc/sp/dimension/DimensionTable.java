package com.alibaba.tc.sp.dimension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.tc.SystemProperty.DEBUG;

public abstract class DimensionTable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    protected volatile TableIndex tableIndex;

    /**
     * for only care current data condition use this method to wait the dimension table finished loading then begin to consume
     * from upstream on current time make the task delay rapidly decrease
     */
    public void waitForReady() {
        while (null == tableIndex) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * <p>
     * use like below example to avoid data inconsistent problem:
     * <pre>
TableIndex tableIndex = dimensionTable.curTable();
table = table.leftJoin(tableIndex.getTable(), new JoinCriteria() {
    <literal>@Override</literal>
    public{@code List<Integer>} theOtherRows(Row thisRow) {
        // Use tableIndex.getRows but not
        // mysqlDimensionTable.curTable().getRows. Consider the second
        // mysqlDimensionTable.curTable() may correspond to the newly
        // reloaded dimension table which is not consistent with the first
        // mysqlDimensionTable.curTable() and tableIndex.getTable()
        return tableIndex.getRows(...);
     }
}...
     * </pre>
     * </p>
     * @return the newest TableIndex
     */
    public TableIndex curTable() {
        waitForReady();
        return tableIndex;
    }

    protected boolean debug(int row) {
        if (DEBUG && row > 100_000) {
            return true;
        }

        return false;
    }
}
