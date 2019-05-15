package com.flink.examples.source;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class UserParallerSource implements ParallelSourceFunction<String>{

	private static final long serialVersionUID = -5588868565003762740L;
	private boolean isRunning=true;
	private static String[] names={"zs","ls","ww"};
	private Random random=new Random();
	@Override
	public void run(SourceContext<String> ctx)
			throws Exception {
		while(isRunning){
			Thread.sleep(1000);
			ctx.collect(names[random.nextInt(3)]);
		}
	}

	@Override
	public void cancel() {
		isRunning=false;
	}

}
