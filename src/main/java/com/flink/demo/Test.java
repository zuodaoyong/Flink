package com.flink.demo;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSONObject;

public class Test {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> streamSource = environment.readTextFile("hdfs://spark1:8020/flink/demo/user.txt");
		
		SingleOutputStreamOperator<User> reduce = streamSource.map(new MapFunction<String,User>() {
			private static final long serialVersionUID = 1936417278744213107L;

			@Override
			public User map(String value) throws Exception {
				return JSONObject.parseObject(value, User.class);
			}
		})
		.keyBy("age")
		.reduce(new ReduceFunction<User>() {
			
			@Override
			public User reduce(User value1, User value2) throws Exception {
				String name=value1.getName()+"&"+value2.getName();
				value1.setName(name);
				return value1;
			}
		});
		reduce.writeAsText("hdfs://spark1:8020/flink/demo/test.txt");
		environment.execute();
	}
}
