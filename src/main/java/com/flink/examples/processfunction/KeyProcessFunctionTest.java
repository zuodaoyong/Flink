package com.flink.examples.processfunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入的值：key 数字
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<String> operator = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split("\\s");
                return new Tuple2<String, Integer>(split[0], Integer.valueOf(split[1]));
            }
        }).keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, String>() {
                    private long lazyTime = 10 * 1000;

                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<String> collector) throws Exception {
                        Integer value = stringIntegerTuple2.f1;
                        if (value > 10) {//添加timer
                            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + lazyTime);
                        }
                        collector.collect(stringIntegerTuple2.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "超过阈值");
                    }
                });
        operator.print();
        environment.execute(KeyProcessFunctionTest.class.getSimpleName());

    }
}
