package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.examples.source.ParallerRandomSource;


public class ParallerSourceApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.addSource(new ParallerRandomSource())
		.map(new MapFunction<Integer, Integer>() {
			private static final long serialVersionUID = 7289263677160698867L;
			@Override
			public Integer map(Integer value) throws Exception {
				return value;
			}
		}).print();
		environment.execute("ParallerSourceApplication");
	}
}
