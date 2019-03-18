package com.flink.examples.batch;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * 累加器
 */
public class CounterApplication {

	public static void main(String[] args) throws Exception {
		List<Integer> list=Arrays.asList(new Integer[]{1,2,3,4,5,6,7,8,9});
		ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Integer> dataSource = environment.fromCollection(list);
		dataSource.map(new RichMapFunction<Integer, Integer>() {

			//创建累加器
			private IntCounter counter=new IntCounter();
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				//注册累加器
				getRuntimeContext().addAccumulator("counter", counter);
			}
			int sum=0;
			@Override
			public Integer map(Integer value) throws Exception {
				this.counter.add(value);
				sum++;
				if(value==9){
					System.out.println(Thread.currentThread().getId()+"num="+sum);
				}
				return value;
			}
		}).setParallelism(1)
		.writeAsText("D://asd.txt");
		JobExecutionResult execute = environment.execute(CounterApplication.class.getSimpleName());
		//获取累加器
		Object result = execute.getAccumulatorResult("counter");
		System.out.println(result);
	}
}
