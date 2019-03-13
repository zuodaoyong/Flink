package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.flink.examples.source.RichParallerSource;

/**
 * union
 * 合并多个流，新的流会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致的
 */
public class UnionStreamApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> source1 = environment.addSource(new RichParallerSource()).setParallelism(1);
		DataStreamSource<Integer> source2 = environment.addSource(new RichParallerSource()).setParallelism(1);
		DataStream<Integer> unionStream = source1.union(source2);
		unionStream.map(new MapFunction<Integer,Integer>() {

			@Override
			public Integer map(Integer value) throws Exception {
				System.out.println("线程:"+Thread.currentThread().getId()+" 消费"+value);
				return value;
			}
		}).setParallelism(2).timeWindowAll(Time.seconds(2)).sum(0).print().setParallelism(1);
		environment.execute(UnionStreamApplication.class.getSimpleName());
	}
}
