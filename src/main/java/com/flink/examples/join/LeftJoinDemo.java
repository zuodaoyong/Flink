package com.flink.examples.join;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class LeftJoinDemo {


    public static void main(String[] args) throws Exception{
        long delay=5002;
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<Tuple3<String, String, Long>> sourceA = env.addSource(new StreamDataSourceA()).name("sourceA");
        DataStream<Tuple3<String, String, Long>> sourceB = env.addSource(new StreamDataSourceB()).name("sourceB");
        DataStream<Tuple3<String, String, Long>> leftStream = sourceA.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3) {
                return stringStringLongTuple3.f2;
            }
        });
        DataStream<Tuple3<String, String, Long>> rightStream = sourceB.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3) {
                return stringStringLongTuple3.f2;
            }
        });
        leftStream.coGroup(rightStream)
                .where(new KeySelector<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                        return stringStringLongTuple3.f0;
                    }
                }).equalTo(new KeySelector<Tuple3<String, String, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                return stringStringLongTuple3.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> left, Iterable<Tuple3<String, String, Long>> right, Collector<Tuple5<String, String, String, Long, Long>> collector) throws Exception {
                        for(Tuple3<String, String, Long> leftElement:left){
                            boolean flag=false;
                            for(Tuple3<String, String, Long> rightElement:right){
                               collector.collect(Tuple5.of(leftElement.f0,leftElement.f1,rightElement.f1,leftElement.f2,rightElement.f2));
                               flag=true;
                            }
                            if(!flag){
                                collector.collect(Tuple5.of(leftElement.f0,leftElement.f1,"null",leftElement.f2,-1L));
                            }
                        }
                    }
                }).print();

        env.execute(LeftJoinDemo.class.getSimpleName());
    }
}
