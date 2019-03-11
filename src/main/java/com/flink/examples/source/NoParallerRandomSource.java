package com.flink.examples.source;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 非并行流
 * @author zdy48195
 *
 */
public class NoParallerRandomSource implements SourceFunction<Integer>{
	
	private static final long serialVersionUID = -5044599222676667873L;
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
		isRunning=false;
	}

}
