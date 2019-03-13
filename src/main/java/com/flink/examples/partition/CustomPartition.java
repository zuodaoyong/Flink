package com.flink.examples.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartition implements Partitioner<Integer>{

	private static final long serialVersionUID = -1434005704951781925L;

	@Override
	public int partition(Integer key, int numPartitions) {
		System.out.println("分区总数："+numPartitions);
		if(key%2==0){
			return 0;
		}else{
			return 1;
		}
	}

}
