package com.flink.examples.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.flink.examples.source.RichParallerSource;
import com.flink.examples.source.UserParallerSource;

public class BroadcastApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		//broadcastPartition(environment);
		broadcastVariable(environment);
		environment.execute(BroadcastApplication.class.getSimpleName());
	}
	
	/**
	 * broadcast分区
	 */
	public static void broadcastPartition(StreamExecutionEnvironment environment){
		DataStreamSource<Integer> source = environment.addSource(new RichParallerSource()).setParallelism(1);
		source.broadcast().map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				System.out.println("线程:"+Thread.currentThread().getId()+"消费:"+value);
				return value;
			}
		}).print().setParallelism(1);
	}
	
	/**
	 * broadcast变量
	 */
	public static void broadcastVariable(StreamExecutionEnvironment environment){
		List<Tuple2<String,Integer>> broadData=new ArrayList<>();
		broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("broadCastMapName", String.class,Integer.class);
        DataStreamSource<Tuple2<String, Integer>> broadDataStream = environment.fromCollection(broadData);
        BroadcastStream<Tuple2<String, Integer>> broadcastStream = broadDataStream.broadcast(descriptor);
        //准备原数据
		DataStreamSource<String> source = environment.addSource(new UserParallerSource()).setParallelism(1);
		source.connect(broadcastStream).process(new BroadcastProcessFunction<String,Tuple2<String,Integer>,String>() {

			@Override
			public void processBroadcastElement(Tuple2<String, Integer> broadTuple,Context ctx,Collector<String> collector) throws Exception {
				//接受broadcast变量
				ctx.getBroadcastState(descriptor).put(broadTuple.f0, broadTuple.f1);
			}

			@Override
			public void processElement(String element,ReadOnlyContext ctx,Collector<String> collector) throws Exception {
				ReadOnlyBroadcastState<String,Integer> broadcastState = ctx.getBroadcastState(descriptor);
				Integer age = broadcastState.get(element);
				System.out.println("姓名:"+element+",年龄:"+age);
				collector.collect("姓名:"+element+",年龄:"+age);
			}

			
		}).print().setParallelism(1);
	}
}
