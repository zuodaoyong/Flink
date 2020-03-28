package com.flink.examples.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class InnerJoinDemo {


    public static void main(String[] args) throws Exception{
        long delay=5012L;
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
        leftStream.join(rightStream)
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
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String,String,String,Long,Long>>() {
                    @Override
                    public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right) throws Exception {
                        return new Tuple5<>(left.f0,left.f1,right.f1,left.f2,right.f2);
                    }
                }).print();

        env.execute(InnerJoinDemo.class.getSimpleName());
    }
}
