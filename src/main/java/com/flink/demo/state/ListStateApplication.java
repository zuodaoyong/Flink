package com.flink.demo.state;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

public class ListStateApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.fromElements(Tuple2.of(1L, 3L), Tuple2.of(2L, 5L), Tuple2.of(2L, 7L), Tuple2.of(4L, 4L), Tuple2.of(5L, 2L))
		.keyBy(0)
		.flatMap(new RichFlatMapFunction<Tuple2<Long,Long>, Tuple2<Long,Long>>() {

			private transient ListState<Tuple2<Long, Long>> keyStates;
			@Override
			public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long,Long>> out) throws Exception {
				boolean flag=false;
				Iterable<Tuple2<Long, Long>> iterable = keyStates.get();
				Iterator<Tuple2<Long, Long>> iterator = iterable.iterator();
				if(!iterator.hasNext()){
					Tuple2<Long, Long> key=new Tuple2<Long, Long>();
					key.f0=value.f0;
					key.f1=1L;
					keyStates.add(key);
				}else{
					while(iterator.hasNext()){
						Tuple2<Long,Long> next = iterator.next();
						if(next.f0.longValue()==value.f0.longValue()){
							flag=true;
							next.f1+=1;
							break;
						}
					}
					if(!flag){
						Tuple2<Long, Long> key=new Tuple2<Long, Long>();
						key.f0=value.f0;
						key.f1=1L;
						keyStates.add(key);
					}
				}
				ArrayList<Tuple2<Long, Long>> list = Lists.newArrayList(keyStates.get());
				System.out.println("keyStates="+list);
				out.collect(value);
			}
			
			@Override
			public void open(Configuration parameters) throws Exception {
				ListStateDescriptor<Tuple2<Long, Long>> descriptor =
		                new ListStateDescriptor<>(
		                        "keys", // 状态名称
		                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // 状态默认值
				keyStates = getRuntimeContext().getListState(descriptor);
			}
		})
		.print();
		environment.execute("listStateApplication");
	}
}
