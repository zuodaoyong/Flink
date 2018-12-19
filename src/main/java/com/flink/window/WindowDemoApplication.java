package com.flink.window;

import java.util.Iterator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;

public class WindowDemoApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> streamSource = environment.socketTextStream("home0",8000);
		environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		streamSource.map(new MapFunction<String,Log>() {

			@Override
			public Log map(String line) throws Exception {
				return JSON.parseObject(line, Log.class);
			}
		}).keyBy("id")
		.timeWindow(Time.minutes(1))
		.reduce(new ReduceFunction<Log>() {
			private static final long serialVersionUID = -8176662782779861564L;
			@Override
			public Log reduce(Log value1, Log value2) throws Exception {
				int res=value1.getUv()+value2.getUv();
				Log log = new Log();
				log.setId(value1.getId());
				log.setUv(res);
				System.out.println(log);
				return log;
			}
		});
		
		/*.apply(new WindowFunction<Log,Tuple2<String,Integer>, Tuple, TimeWindow>() {

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Log> input, Collector<Tuple2<String, Integer>> out)
					throws Exception {
				Iterator<Log> iterator = input.iterator();
				int count=0;
				while(iterator.hasNext()){
					Log next = iterator.next();
					count+=next.getUv();
				}
				Tuple2 res=new Tuple2(key.getField(0).toString(),count);
				out.collect(res);
				System.out.println(window.getStart()+"~"+window.getEnd());
				System.out.println(key+",res="+res);
			}

		});*/
		environment.execute("WindowDemoApplication");
	}
}
