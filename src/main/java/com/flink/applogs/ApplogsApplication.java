package com.flink.applogs;

import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import com.alibaba.fastjson.JSON;

public class ApplogsApplication {

	public static void main(String[] args) {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		FlinkKafkaConsumer09<String> consumer08=new FlinkKafkaConsumer09<>("topic-app-startup", new SimpleStringSchema(), getProperties());
		DataStreamSource<String> streamSource = environment.addSource(consumer08);
		
		 WindowedStream<AppStartupLog, String, GlobalWindow> countWindow = streamSource.map(new MapFunction<String,AppStartupLog>() {
			private static final long serialVersionUID = -6560636050605101252L;

			@Override
			public AppStartupLog map(String line) throws Exception {
				return JSON.parseObject(line, AppStartupLog.class);
			}
		}).keyBy(new KeySelector<AppStartupLog,String>() {

			@Override
			public String getKey(AppStartupLog log) throws Exception {
				return log.getAppPlatform();
			}
		}).countWindow(10,10);
		 
		//keyedStream.writeAsText("D:\\software\\temp\\flink\\1.txt");
		try {
			environment.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static Properties getProperties(){
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "home0:9092,home1:9092,home2:9092");
		properties.setProperty("zookeeper.connect", "home0:2181,home1:2181,home2:2181");
		properties.setProperty("group.id", "test");
		return properties;
	}
}
