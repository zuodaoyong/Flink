package com.flink.examples.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.examples.source.RichParallerSource;

/**
 * 对流进行切分，按照数据的奇偶性进行区分
 */
public class SplitStreamApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setParallelism(1);
		SplitStream<Integer> splitStream = environment.addSource(new RichParallerSource())
		           .split(new OutputSelector<Integer>() {
					
					@Override
					public Iterable<String> select(Integer value) {
						List<String> outPut = new ArrayList<>();
						if(value%2==0){
							outPut.add("even");//偶数
						}else{
							outPut.add("odd");//奇数
						}
						return outPut;
					}
				});
		//选择一个或者多个切分后的流
		DataStream<Integer> evenStream = splitStream.select("even");
		//DataStream<Integer> moreStream = splitStream.select("even","odd");
		evenStream.print().setParallelism(2);
		environment.execute(SplitStreamApplication.class.getSimpleName());
	}
}
