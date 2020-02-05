package com.flink.project.userBehavior.hotItem;

import com.flink.project.userBehavior.entity.ItemViewCount;
import com.flink.project.userBehavior.entity.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class HotItemApplication {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> streamSource = environment.readTextFile("D:\\temp\\flink\\data\\UserBehavior.csv");
        DataStream<UserBehavior> behaviorStream = streamSource.map(new HotItemMapFunction());
        WindowedStream<UserBehavior, Long, TimeWindow> windowedStream = behaviorStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                if (userBehavior.getBehavior().equals("pv")) {
                    return true;
                }
                return false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestramp()*1000L;
            }
        }).keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior userBehavior) throws Exception {
                return userBehavior.getItemId();
            }
        }).timeWindow(Time.hours(1), Time.minutes(5));

        DataStream<ItemViewCount> aggregateStream = windowedStream.aggregate(new AggCountFunction(), new HotItemTimeWindowFunction());
        //排序
        aggregateStream.keyBy(new KeySelector<ItemViewCount, Long>() {
            @Override
            public Long getKey(ItemViewCount itemViewCount) throws Exception {
                return itemViewCount.getWindowEnd();
            }
        }).process(new HotItemProcessFunction(3)).print();
        environment.execute(HotItemApplication.class.getSimpleName());
    }
}
