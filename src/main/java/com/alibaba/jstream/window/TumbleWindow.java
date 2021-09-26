package com.alibaba.jstream.window;

import com.alibaba.jstream.function.AggTimeWindowFunction;
import com.alibaba.jstream.function.TimeWindowFunction;
import com.alibaba.jstream.table.Table;

import java.time.Duration;
import java.util.List;

public class TumbleWindow {
    private final SlideWindow slideWindow;

    public TumbleWindow(Duration windowSize,
                     String[] partitionByColumnNames,
                     String timeColumnName,
                     AggTimeWindowFunction aggTimeWindowFunction,
                     String... columnNames) {
        this.slideWindow = new SlideWindow(windowSize,
                windowSize,
                partitionByColumnNames,
                timeColumnName,
                aggTimeWindowFunction,
                columnNames);
    }

    public TumbleWindow(Duration windowSize,
                     String[] partitionByColumnNames,
                     String timeColumnName,
                     TimeWindowFunction windowFunction,
                     String... addedColumnNames) {
        this.slideWindow = new SlideWindow(windowSize,
                windowSize,
                partitionByColumnNames,
                timeColumnName,
                windowFunction,
                addedColumnNames);
    }

    public void setNoDataDelay(Duration noDataDelay) {
        slideWindow.setNoDataDelay(noDataDelay);
    }

    public void setWatermark(Duration watermark) {
        slideWindow.setWatermark(watermark);
    }

    public Table tumble(List<Table> tables) {
        return slideWindow.slide(tables);
    }
}
