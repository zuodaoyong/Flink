package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.examples.partition.CustomPartition;
import com.flink.examples.source.RichParallerSource;


/**
 * 自定义分区
 * @author zdy48195
 *
 */
public class CustomParititionApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment(); 
		environment.setParallelism(3);
		DataStream<Integer> stream = environment.addSource(new RichParallerSource());
		stream.map(new MapFunction<Integer, Tuple1<Integer>>() {

			@Override
			public Tuple1<Integer> map(Integer value) throws Exception {
				return new Tuple1<>(value);
			}
		}).partitionCustom(new CustomPartition(), 0)
		.map(new MapFunction<Tuple1<Integer>, Integer>() {

			@Override
			public Integer map(Tuple1<Integer> value) throws Exception {
				System.out.println(Thread.currentThread().getId()+"消费:"+value.f0);
				return value.f0;
			}
		}).print().setParallelism(1);
		
		environment.execute(CustomParititionApplication.class.getSimpleName());
	}
}
