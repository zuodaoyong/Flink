package com.flink.examples.tableApi;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class TableApiTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<String> streamSource = env.fromCollection(Arrays.asList(new String[]{"spark flink", "hadoop netty flink"}));
        DataStream<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split("\\s");
                for (String str : split) {
                    collector.collect(str);
                }
            }
        });
        tableEnvironment.registerDataStream("wordcount",flatMap,"word");
        Table table = tableEnvironment.sqlQuery("select word,count(1) counts from wordcount group by word");

        DataStream<Tuple2<Boolean, WordCount>> tuple2DataStream = tableEnvironment.toRetractStream(table, WordCount.class);
        tuple2DataStream.print();
        env.execute(TableApiTest.class.getSimpleName());
    }


}
