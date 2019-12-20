package com.flink.examples.processfunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideoutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入的值：key 数字
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 7777);

        DataStream<Tuple2<String, Integer>> outputTest = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split("\\s");
                return new Tuple2<String, Integer>(split[0], Integer.valueOf(split[1]));
            }
        }).process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Integer value = stringIntegerTuple2.f1;
                if (value > 10) {
                    OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("outputTest_tuple"){};
                    context.output(outputTag, stringIntegerTuple2);
                } else if(value>5) {
                    OutputTag<String> outputTag = new OutputTag<String>("outputTest_String"){};
                    context.output(outputTag, stringIntegerTuple2.f0);
                }else {
                    collector.collect(stringIntegerTuple2);
                }
            }
        });
        //根据outputTest_tuple取Tuple类型的侧输出流
        DataStream<Tuple2<String, Integer>> sideOutput_tuple =  ((SingleOutputStreamOperator<Tuple2<String, Integer>>) outputTest).getSideOutput(new OutputTag<Tuple2<String, Integer>>("outputTest_tuple"){});
        sideOutput_tuple.print();
        //根据outputTest_String取String类型的侧输出流
        DataStream<String> sideOutput_String =  ((SingleOutputStreamOperator<Tuple2<String, Integer>>) outputTest).getSideOutput(new OutputTag<String>("outputTest_String"){});
        sideOutput_String.print();
        environment.execute(SideoutputTest.class.getSimpleName());
    }
}
