package com.flink.demo;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * watermark时间 >= window_end_time
 * 在[window_start_time,window_end_time)中有数据存在
 * @author zdy48195
 *https://blog.csdn.net/xu470438000/article/details/83271123
 */
public class WaterMarkApplication {

	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		environment.setParallelism(1);
		
		DataStreamSource<String> textStream = environment.socketTextStream("192.168.237.138",8888);
		textStream.map(new MapFunction<String,Tuple2<String,Long>>() {
			private static final long serialVersionUID = 3753304028813911473L;
			@Override
			public Tuple2<String, Long> map(String line) throws Exception {
				String[] split = line.split(",");
				return new Tuple2<String,Long>(split[0],Long.valueOf(split[1]));
			}
		}).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>(){

			Long currentMaxTime=0L;
			Long maxOutOfOrder=10000L;
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			@Override
			public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
				long timestamp=element.f1;
				currentMaxTime=Math.max(timestamp, currentMaxTime);
                String message="key:"+element.f0+",event time:"+element.f1+"|"+sdf.format(element.f1)
                +"currentMaxTime:"+currentMaxTime+"|"+sdf.format(currentMaxTime)
                +",water mark:"+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp());
		        System.out.println(message);
				return timestamp;
			}

			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(currentMaxTime-maxOutOfOrder);
			}
		})
		.keyBy(0)
		.window(TumblingEventTimeWindows.of(Time.seconds(3)))
		.allowedLateness(Time.seconds(2))
		.apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
			private static final long serialVersionUID = -711467103072921126L;
			@Override
			public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out)
					throws Exception {
				String key = tuple.toString();
                List<Long> arrarList = new ArrayList<Long>();
                Iterator<Tuple2<String, Long>> it = input.iterator();
                while (it.hasNext()) {
                    Tuple2<String, Long> next = it.next();
                    arrarList.add(next.f1);
                } 
                Collections.sort(arrarList);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf.format(arrarList.get(arrarList.size() - 1))
                        + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                out.collect(result);
			}
		}).print();
		environment.execute("WaterMarkApplication");
	}
}
