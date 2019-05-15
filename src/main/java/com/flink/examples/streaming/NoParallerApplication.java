package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.examples.source.NoParallerRandomSource;

/**
 * 非并行流测试
 *
 */
public class NoParallerApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.addSource(new NoParallerRandomSource())
		.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return value;
			}
		}).print();
		environment.execute("NoParallerApplication");
	}
}
