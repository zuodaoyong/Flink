package com.flink.examples.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class RichParallerSource extends RichParallelSourceFunction<Integer>{

	private static final long serialVersionUID = 1153531717038544222L;
    private int count=1;
	private boolean isRunning=true;
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		System.out.println("打开资源....");
	}
	
	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		super.close();
		System.out.println("关闭资源....");
	}
	
	@Override
	public void run(SourceContext<Integer> ctx)
			throws Exception {
		while(isRunning){
			Thread.sleep(1000);
			ctx.collect(count++);
		}
	}

	@Override
	public void cancel() {
		isRunning=false;
	}

}
