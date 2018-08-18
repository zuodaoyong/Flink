package com.flink.demo.wordcount;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> streamSource = environment.addSource(new WordSource(),"abc");
		streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				String[] split = value.split(" ");
				Arrays.asList(split).stream()
				.forEach(e->{
					out.collect(new Tuple2<String, Integer>(e, 1));
				});
			}
		})
		.keyBy(0)
		.sum(1)
		.print();
		environment.execute();
		
		/*streamSource.flatMap((input,out)->{
			String[] split = input.split(" ");
			Arrays.asList(split).stream()
			.forEach(e->{
				out.collect(new Tuple2<String, Integer>(e, 1));
			});
		});*/
		
	}
}
