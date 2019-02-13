package com.flink.demo.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReducingStateApplication {

	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.fromElements(Tuple2.of(1L, 1L), Tuple2.of(1L, 1L),Tuple2.of(2L, 1L),Tuple2.of(1L, 1L),Tuple2.of(2L, 1L))
		.keyBy(0)
		.flatMap(new RichFlatMapFunction<Tuple2<Long,Long>, Tuple2<Long,Long>>() {
			private transient ReducingState<Tuple2<Long,Long>> reducingState;
			@Override
			public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
				reducingState.add(value);
				System.out.println("reducingState="+reducingState.get());
				out.collect(value);
			}
			
			@Override
			public void open(Configuration parameters) throws Exception {
				ReducingStateDescriptor<Tuple2<Long, Long>> descriptor
				=new ReducingStateDescriptor<>("reducingState", new ReduceFunction<Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2)
							throws Exception {
						if(value2!=null){
							if(value1.f0.longValue()==value2.f0.longValue()){
								value1.f1+=1;
							}
						}
						return value1;
					}
				}, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));
				reducingState = getRuntimeContext().getReducingState(descriptor);
			}
		})
		.print();
		environment.execute("reducingStateApplication");
	}
}
