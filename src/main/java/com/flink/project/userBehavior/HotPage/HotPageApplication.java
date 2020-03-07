package com.flink.project.userBehavior.HotPage;

import com.flink.project.userBehavior.entity.ApacheLog;
import com.flink.project.userBehavior.entity.HotPageViewCount;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

public class HotPageApplication {


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);
        DataStream<String> sourceStream=env.readTextFile("D:\\temp\\flink\\data\\apache.log");
        sourceStream.flatMap(new HotPageFlatMapFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLog apacheLog) {
                        return apacheLog.getEventTime();
                    }


                })
                .keyBy(new KeySelector<ApacheLog, String>() {
                    @Override
                    public String getKey(ApacheLog apacheLog) throws Exception {
                        return apacheLog.getUrl();
                    }
                })
                .timeWindow(Time.minutes(10),Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .aggregate(new HotPageAggCountFunction(),new HotPageTimeWindowFunction())
                .keyBy(new KeySelector<HotPageViewCount, Long>() {
                    @Override
                    public Long getKey(HotPageViewCount hotPageViewCount) throws Exception {
                        return hotPageViewCount.getWindowEnd();
                    }
                }).process(new HotPageProcessFunction()).writeAsText("D://temp//window.txt").setParallelism(1);

        env.execute(HotPageApplication.class.getSimpleName());
    }
}
