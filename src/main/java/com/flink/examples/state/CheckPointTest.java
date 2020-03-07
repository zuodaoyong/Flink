package com.flink.examples.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CheckPointTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMinPauseBetweenCheckpoints(3000);
        checkpointConfig.setFailOnCheckpointingErrors(false);
        //设置重启策略
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(2)));
        //当job被cancel或者程序出现异常则会删除checkpoint状态，如果想下次启动时恢复数据，则要设置不清除checkpoint
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        environment.setStateBackend(new FsStateBackend("hdfs://master:9000/flink/checkpoints",true));

        DataStreamSource<String> streamSource = environment.socketTextStream("master", 8888);
        streamSource.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if(s.equals("error")){
                    throw new RuntimeException("数据错误");
                }
                return new Tuple2<>(s,1);
            }
        }).keyBy(0).sum(1).print();

        environment.execute(CheckPointTest.class.getSimpleName());
    }
}
