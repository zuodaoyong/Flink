package com.flink.demo.state;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapStateApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.fromElements(Tuple2.of(1L, 1L), Tuple2.of(2L, 1L), Tuple2.of(4L, 1L), Tuple2.of(5L, 1L),Tuple2.of(2L, 1L))
		.keyBy(0)
		.flatMap(new RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>>() {

			private transient MapState<Long,Long> mapState;
			@Override
			public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
				Iterable<Entry<Long, Long>> iterable = mapState.entries();
				Iterator<Entry<Long, Long>> iterator = iterable.iterator();
				if(!iterator.hasNext()){
					mapState.put(value.f0, value.f1);
				}else{
					while(iterator.hasNext()){
						Entry<Long, Long> entry = iterator.next();
						if(entry.getKey().longValue()==value.f0.longValue()){
							entry.setValue(entry.getValue()+value.f1);
							break;
						}
					}
				}
				Thread.sleep(1000);
				System.out.println("mapState="+Lists.newArrayList(mapState.iterator()));
				out.collect(value);
			}
			
			@Override
			public void open(Configuration parameters) throws Exception {
				MapStateDescriptor<Long,Long> descriptor=
						new MapStateDescriptor<>("mapState", TypeInformation.of(new TypeHint<Long>() {}), TypeInformation.of(new TypeHint<Long>() {}));
				StateTtlConfig ttlConfig=StateTtlConfig.newBuilder(Time.seconds(2))
						.setUpdateType(UpdateType.OnCreateAndWrite)
						.setStateVisibility(StateVisibility.NeverReturnExpired)
						.build();
				descriptor.enableTimeToLive(ttlConfig);
				mapState=getRuntimeContext().getMapState(descriptor);
				
				
			}
		})
		.print();
		environment.execute("mapStateApplication");
	}
}
