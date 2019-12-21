package com.flink.examples.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StateTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("localhost", 7777);
        //输入的值：key 数字
        DataStream<Tuple3<String,Integer,Integer>> tuple3DataStream=streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split("\\s");
                return new Tuple2<String, Integer>(split[0], Integer.valueOf(split[1]));
            }
        }).keyBy(0).process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple3<String,Integer,Integer>>() {

            //定义一个状态变量，保存上一次的温度值
            ValueState<Integer> valueState;
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState",Integer.class));
            }

            Integer threshold=10;

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                Integer pre_value=valueState.value();
                valueState.update(value.f1);
                if(pre_value!=null){
                    //用当前值与pre_value差值超过阈值
                    if(Math.abs(value.f1-pre_value)>threshold){
                        out.collect(new Tuple3<>(value.f0,value.f1,pre_value));
                    }
                }
            }
        });
        tuple3DataStream.print();
        environment.execute(StateTest.class.getSimpleName());
    }
}
