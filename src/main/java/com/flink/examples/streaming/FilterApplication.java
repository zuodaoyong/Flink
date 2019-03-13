package com.flink.examples.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.examples.source.RichParallerSource;

public class FilterApplication {
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setParallelism(3);
		environment.addSource(new RichParallerSource())
		           .filter(new FilterFunction<Integer>() {
					@Override
					public boolean filter(Integer value) throws Exception {
                        System.out.println("线程:"+Thread.currentThread().getId());						
						if(value%2==0){
							return true;
						}
						return false;
					}
				}).print().setParallelism(4);
		environment.execute(FilterApplication.class.getSimpleName());
	}

	
}
