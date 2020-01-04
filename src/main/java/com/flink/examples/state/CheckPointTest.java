package com.flink.examples.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CheckPointTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
//        checkpointConfig.setMaxConcurrentCheckpoints(1);
//        checkpointConfig.setCheckpointTimeout(60000);
//        checkpointConfig.setMinPauseBetweenCheckpoints(3000);
//        checkpointConfig.setFailOnCheckpointingErrors(false);
//        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        environment.setStateBackend(new FsStateBackend("hdfs://master:9000/flink/checkpoint",true));
        DataStreamSource<String> socketTextStream = environment.socketTextStream("localhost", 7777);
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                String[] split = s.split("\\s");
                for(String str:split){
                    collector.collect(new Tuple2<String,Integer>(str,1));
                }
            }
        }).keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
                    private ValueState<Tuple2<String,Integer>> valueState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Tuple2<String,Integer>> descriptor =
                                new ValueStateDescriptor<>(
                                        "count", // the state name
                                        TypeInformation.of(new TypeHint<Tuple2<String,Integer>>() {}));
                        valueState=getRuntimeContext().getState(descriptor);
                        //valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count",Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Tuple2<String, Integer> tuple2 = valueState.value();
                        tuple2.f1++;
                        valueState.update(tuple2);
                        collector.collect(stringIntegerTuple2);
                    }
                }).print();

        environment.execute(CheckPointTest.class.getSimpleName());
    }
}
