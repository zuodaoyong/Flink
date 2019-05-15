package com.flink.examples.source;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class ParallerRandomSource implements ParallelSourceFunction<Integer>{

	private static final long serialVersionUID = 2682003020340259550L;

	private boolean isRunning=true;
	private Random random=new Random();
	@Override 
	public void run(SourceContext<Integer> ctx)
			throws Exception {
		while(isRunning){
			Thread.sleep(1000);
			ctx.collect(random.nextInt(100));
		}
	}

	@Override
	public void cancel() {
		
	}

}
