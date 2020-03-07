package com.flink.project.userBehavior.HotPage;


import com.flink.project.userBehavior.entity.HotPageViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HotPageTimeWindowFunction implements WindowFunction<Integer, HotPageViewCount,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Integer> iterable, Collector<HotPageViewCount> collector) throws Exception {
        HotPageViewCount hotPageViewCount=new HotPageViewCount();
        hotPageViewCount.setCount(iterable.iterator().next());
        hotPageViewCount.setUrl(s);
        hotPageViewCount.setWindowEnd(timeWindow.getEnd());
        collector.collect(hotPageViewCount);
    }

}
