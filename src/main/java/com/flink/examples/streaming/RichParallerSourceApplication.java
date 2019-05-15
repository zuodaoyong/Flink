package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.examples.source.RichParallerSource;

public class RichParallerSourceApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.addSource(new RichParallerSource()).setParallelism(3)
		.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				System.out.println("Thread:"+Thread.currentThread().getId());
				return value;
			}
		}).setParallelism(2).print().setParallelism(1);
		environment.execute(RichParallerSourceApplication.class.getSimpleName());
	}
}
