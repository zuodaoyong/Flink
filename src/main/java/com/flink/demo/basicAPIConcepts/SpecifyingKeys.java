package com.flink.demo.basicAPIConcepts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SpecifyingKeys {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> dataSet = environment.readTextFile("D://web.log", "utf-8");
		dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				String[] split = value.split("\\w+");
				for (String e : split) {
					out.collect(new Tuple2<String, Integer>(e, 1));
				}
			}
		}).groupBy(0).reduceGroup(new DistinctReduce()).print();
	}
}

class DistinctReduce implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

	@Override
	public void reduce(Iterable<Tuple2<String, Integer>> in, Collector<Tuple2<String, Integer>> out) {

		Map<String,Integer> uniqStrings = new HashMap<String,Integer>();
		String key = null;
        Integer value=null;
		// add all strings of the group to the set
		for (Tuple2<String, Integer> t : in) {
			key = t.f0;
			value = t.f1;
			if(uniqStrings.get(key)!=null){
				uniqStrings.put(key, uniqStrings.get(key)+value);
			}else{
				uniqStrings.put(key, value);
			}
		}

		// emit all unique strings.
		for (Map.Entry<String, Integer> s : uniqStrings.entrySet()) {
			out.collect(new Tuple2<String, Integer>(s.getKey(), s.getValue()));
		}
	}
}