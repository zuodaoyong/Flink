package com.flink.project.userBehavior.hotItem;

import com.flink.project.userBehavior.entity.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HotItemTimeWindowFunction implements WindowFunction<Long, ItemViewCount,Long,TimeWindow> {

    @Override
    public void apply(Long aLong, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
        Long itemId = aLong;
        Long next = iterable.iterator().next();
        long windowEnd = timeWindow.getEnd();
        ItemViewCount itemViewCount=new ItemViewCount();
        itemViewCount.setItemId(itemId);
        itemViewCount.setCount(next.intValue());
        itemViewCount.setWindowEnd(windowEnd);
        collector.collect(itemViewCount);
    }
}
