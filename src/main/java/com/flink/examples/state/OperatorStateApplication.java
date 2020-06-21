package com.flink.examples.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorStateApplication {


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,2000));
        env.setStateBackend(new FsStateBackend("file:///D:\\temp\\flink\\checkpoint"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /*DataStreamSource<String> master = env.socketTextStream("master", 8888);
        master.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                if(s.equals("error")){
                    throw new RuntimeException("error");
                }
                return s;
            }
        }).print();*/
        DataStreamSource<Tuple2<String, String>> streamSource = env.addSource(new FileSourceWithState("D:\\temp\\flink\\file"));
        streamSource.print();
        env.execute(OperatorStateApplication.class.getSimpleName());
    }
}
