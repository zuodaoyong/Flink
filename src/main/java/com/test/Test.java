package com.test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource1 = env.generateSequence(0, 5);
        DataStreamSource<Long> streamSource2 = env.generateSequence(6, 10);
        DataStream<Long> union = streamSource1.union(streamSource2);
        union.print();
        env.execute(Test.class.getSimpleName());
    }
}

