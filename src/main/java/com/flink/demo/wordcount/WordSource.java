package com.flink.demo.wordcount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class WordSource implements SourceFunction<String>{

	/**
	 * ��������
	 */
	@Override
	public void run(SourceContext<String> ctx)
			throws Exception {
		while (true) {
			ctx.collect("flink spark storm");
		}
	}

	@Override
	public void cancel() {
		
	}

}
