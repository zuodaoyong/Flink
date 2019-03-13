package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.examples.source.RichParallerSource;

public class BroadcastApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		broadcastPartition(environment);
		environment.execute(BroadcastApplication.class.getSimpleName());
	}
	
	/**
	 * broadcast分区
	 */
	public static void broadcastPartition(StreamExecutionEnvironment environment){
		DataStreamSource<Integer> source = environment.addSource(new RichParallerSource()).setParallelism(1);
		source.broadcast().map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				System.out.println("线程:"+Thread.currentThread().getId()+"消费:"+value);
				return value;
			}
		}).print().setParallelism(1);
	}
}
