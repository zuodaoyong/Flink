package com.flink.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction<Log,Tuple2<String,Integer>,String,TimeWindow>{
	private static final long serialVersionUID = 6918222133309945148L;

	@Override
	public void process(String arg0,
			Context context, Iterable<Log> arg2,
			Collector<Tuple2<String, Integer>> arg3) throws Exception {
		
	}

}
