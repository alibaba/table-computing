package com.alibaba.tc.sp.dimension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.tc.SystemProperty.DEBUG;

public abstract class DimensionTable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    protected volatile TableIndex tableIndex;

    /**
     * 对于只关心当前数据的场景可以使用该方法等待加载完成之后再开始读取上游的当前数据使业务延迟可以快速降下来
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
     * 获取最新的TableIndex，使用方通过使用同一个TableIndex避免join时返回的行号不在right table里或者脏行号的问题
     * @return
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
