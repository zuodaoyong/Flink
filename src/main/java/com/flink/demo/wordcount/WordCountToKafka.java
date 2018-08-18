package com.flink.demo.wordcount;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.util.Collector;

public class WordCountToKafka {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> streamSource = env.readTextFile("D:\\web.log", "utf-8");
		FlinkKafkaProducer08<String> producer08 = new FlinkKafkaProducer08<>("spark1:9092", "flink_wordcount_result", new SimpleStringSchema());
		streamSource.flatMap(new FlatMapFunction<String,Tuple2<String,Long>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
				String[] split = value.split(" ");
				Arrays.asList(split).stream()
				.forEach(e->{
					out.collect(new Tuple2<String, Long>(e,1L));
				});
			}
			
		})
		.keyBy(0)
		.sum(1)
		.map(new MapFunction<Tuple2<String,Long>,String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				return value.toString();
			}
		})
		.addSink(producer08);
		env.execute();
		
	}
	
}