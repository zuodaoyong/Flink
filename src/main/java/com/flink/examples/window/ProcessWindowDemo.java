package com.flink.examples.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ProcessWindowDemo {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("home0", 9000);
        //DataStreamSource<String> streamSource = env.readTextFile("D:\\software\\temp\\flink\\test.txt");
        socketTextStream.map(new MapFunction<String, Tuple2<Long,String>>() {
            @Override
            public Tuple2<Long, String> map(String value) throws Exception {
                return new Tuple2<Long,String>(new Date().getTime(),value);
            }
        }).timeWindowAll(Time.seconds(20))
                .process(new ProcessAllWindowFunction<Tuple2<Long, String>, List<Tuple2<Long,String>>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<Long, String>> elements, Collector<List<Tuple2<Long, String>>> out) throws Exception {
                        List<Tuple2<Long, String>> list=new ArrayList<>();
                        Iterator<Tuple2<Long, String>> iterator = elements.iterator();
                        while (iterator.hasNext()){
                            Tuple2<Long, String> next = iterator.next();
                            list.add(next);
                        }
                        out.collect(list);
                    }
                })
       .addSink(new SinkFunction<List<Tuple2<Long, String>>>() {
           @Override
           public void invoke(List<Tuple2<Long, String>> value, Context context) throws Exception {
               System.out.println(value);
           }
       });

        env.execute(ProcessWindowDemo.class.getSimpleName());

        while (Thread.activeCount()>2){
            Thread.yield();
        }
        System.out.println("结束");
    }
}
