package com.flink.examples.batch;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * distinct
 */
public class DistinctApplication {

	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
		DataSource<String> dataSource = environment.fromElements("hello flink","hello java");
		dataSource.flatMap(new FlatMapFunction<String,Tuple2<String,Integer>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
				String[] split = value.split("\\W+");
				for(String str:split){
					out.collect(new Tuple2<String, Integer>(str, 1));
				}
			}
		}).distinct(0).print();
		
		environment.execute(DistinctApplication.class.getSimpleName());
	}
}
