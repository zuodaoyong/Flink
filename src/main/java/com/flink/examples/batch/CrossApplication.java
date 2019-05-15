package com.flink.examples.batch;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * 笛卡尔积
 */
public class CrossApplication {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
		//tuple2<用户id，用户姓名>
        List<String> names = new ArrayList<>();
        names.add("zs");
        names.add("ww");
        List<Integer> ages = new ArrayList<>();
        ages.add(1);
        ages.add(2);
        DataSource<String> namesDataSource = environment.fromCollection(names);
        DataSource<Integer> agesDataSource = environment.fromCollection(ages);
        DefaultCross<String,Integer> cross = namesDataSource.cross(agesDataSource);
        cross.map(new MapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
				return value;
			}
		}).print();
        environment.execute(CrossApplication.class.getSimpleName());
	}
}
