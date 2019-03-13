package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import com.flink.examples.source.RichParallerSource;

public class ConnectStreamApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> stream1 = environment.addSource(new RichParallerSource()).setParallelism(1);
		DataStream<String> stream2 = environment.addSource(new RichParallerSource()).setParallelism(1).map(new MapFunction<Integer,String>() {
			@Override
			public String map(Integer value) throws Exception {
				return "str_"+value;
			}
		});
		stream1.connect(stream2).map(new CoMapFunction<Integer, String,String>() {

			@Override
			public String map1(Integer value) throws Exception {
				return "stream1_"+value;
			}

			@Override
			public String map2(String value) throws Exception {
				return "stream2_"+value;
			}
		})
		.print().setParallelism(1);
		environment.execute(ConnectStreamApplication.class.getSimpleName());
	}
}
