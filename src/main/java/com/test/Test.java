package com.test;

import com.common.TimeUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;

public class Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());






        DataStreamSource<Long> streamSource = env.generateSequence(0, 10);
        streamSource.map(new MapFunction<Long, Tuple2<Long,Integer>>() {
            @Override
            public Tuple2<Long,Integer> map(Long aLong) throws Exception {
                return new Tuple2<Long,Integer>(aLong,1);
            }
        }).keyBy(0).sum(1);
        //splitStream.select("even").print("even=");
        //splitStream.select("odd").print("odd=");
        //splitStream.select("even","odd").print("even,odd=");
        env.execute(Test.class.getSimpleName());
    }
}

