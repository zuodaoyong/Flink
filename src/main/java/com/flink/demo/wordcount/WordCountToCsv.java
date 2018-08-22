package com.flink.demo.wordcount;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Åú´¦Àí
 * @author zdy48195
 *
 */
public class WordCountToCsv {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> dataSet = env.readTextFile("D://web.log","utf-8");
		DataSet<Tuple2<String, Long>> result = dataSet.flatMap(new FlatMapFunction<String,Tuple2<String,Long>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
				String[] split = value.split("\\W+");
				Arrays.asList(split)
				.stream()
				.forEach(e->{
					out.collect(new Tuple2<String, Long>(e,1L));
				});
			}
		}).groupBy(0).sum(1);
		
		result.writeAsCsv("D://wordcount", "\n", " ");
		env.execute();
	}
}
