package com.flink.examples.batch;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * distributed Cache
 */
public class DistributedCacheApplication {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
		environment.registerCachedFile("D:\\asd.txt", "asd", true);
		DataSource<Integer> dataSource = environment.fromElements(1,2,4);
		
		dataSource.map(new RichMapFunction<Integer,Integer>() {

			private List<String> list;
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				File file = getRuntimeContext().getDistributedCache().getFile("asd");
				list = FileUtils.readLines(file);
			}
			
			@Override
			public Integer map(Integer value) throws Exception {
				System.out.println("list="+list);
				return value;
			}
		}).print();
		
		environment.execute(DistributedCacheApplication.class.getSimpleName());
	}
}
