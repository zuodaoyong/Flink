package com.flink.demo.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 状态管理
 * @author zdy48195
 *
 */
public class ValueStateApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
		environment.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .keyBy(0)
        .flatMap(new RichFlatMapFunction<Tuple2<Long,Long>, Tuple2<Long, Long>>() {
			private static final long serialVersionUID = -7436834458733719684L;
			 /**
		     * ValueState状态句柄. 第一个值为count，第二个值为sum。
		     */
		    private transient ValueState<Tuple2<Long, Long>> sum;
			@Override
			public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
				 // 获取当前状态值
		        Tuple2<Long, Long> currentSum = sum.value();
		        if(currentSum==null){
		        	currentSum=new Tuple2<Long, Long>();
		        	currentSum.f0=1L;
		        	currentSum.f1= input.f1;
		        }else{
		        	// 更新
		        	currentSum.f0 += 1;
		        	currentSum.f1 += input.f1;
		        }
		        // 更新状态值
		        sum.update(currentSum);
		        // 如果count >=2 清空状态值，重新计算
		        if (currentSum.f0 >= 2) {
		            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
		            sum.clear();
		        }
			}
			@Override
			public void open(Configuration parameters) throws Exception {
				 ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
			                new ValueStateDescriptor<>(
			                        "average", // 状态名称
			                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // 状态默认值
			        sum = getRuntimeContext().getState(descriptor);
			}
		})
        .print();
		environment.execute("valueStateApplication");
	}
}
