package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 使用broadcast广播少量数据，用于类似join的操作
 * 注意：broadcast数据量一定要少，不然很占用state的内存
 */
public class BroadCastRichApplication {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        MapStateDescriptor<String,String> mapStateDescriptor=new MapStateDescriptor("config-broadCast",String.class,String.class);
        //定义broadcast流
        DataStreamSource<String> broadCastStream = env.socketTextStream("localhost", 6666);
        BroadcastStream<Tuple2<String,String>> broadcastStream =broadCastStream.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(String s) throws Exception {
                //s为id,name
                String[] split = s.split(",");
                return new Tuple2<>(split[0],split[1]);
            }
        }).broadcast(mapStateDescriptor);//向slot里广播mapState

        //定义数据流
        DataStreamSource<String> streamSource=env.addSource(new SourceFunction<String>(){
            private boolean isRunning=true;
            List<String> seeds= Arrays.asList(new String[]{
                    "1,male","2,female","3,male"
            });
            Random random=new Random();
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
               while (isRunning){
                   Thread.sleep(1000);
                   int i = random.nextInt(3);
                   ctx.collect(seeds.get(i));
               }
            }

            @Override
            public void cancel() {
                isRunning=false;
            }
        });
        DataStream<Tuple2<String,String>> dataStream =streamSource.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(String s) throws Exception {
                //s为id,gender
                String[] split = s.split(",");
                return new Tuple2<>(split[0],split[1]);
            }
        });
        dataStream.print();
        dataStream.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String,String,String>>() {
            @Override
            public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                if(broadcastState.contains(value.f0)){
                    out.collect(new Tuple3<>(value.f0,broadcastState.get(value.f0),value.f1));
                }else{
                    out.collect(new Tuple3<>(value.f0,"",value.f1));
                }
            }

            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put(value.f0,value.f1);
                System.out.println("broadcastst="+broadcastState);
            }
        }).print();

        env.execute(BroadCastRichApplication.class.getSimpleName());
    }
}
