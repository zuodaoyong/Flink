package com.flink.project.order;

import com.flink.project.order.entity.OrderEvent;
import com.flink.project.order.entity.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 订单失效检测
 */
public class OrderLoseDetectApplication {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> sourceStream = env.readTextFile("D:\\temp\\flink\\data\\OrderLog.csv");
        KeyedStream<OrderEvent, String> keyedStream = sourceStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String msg) throws Exception {
                String[] split = msg.split(",");
                return new OrderEvent(split[0], split[1], split[2], Long.valueOf(split[3]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getEventTime() * 1000;
            }
        }).keyBy(new KeySelector<OrderEvent, String>() {

            @Override
            public String getKey(OrderEvent orderEvent) throws Exception {
                return orderEvent.getOrderId();
            }
        });

        Pattern<OrderEvent, OrderEvent> eventPattern = Pattern.<OrderEvent>begin("create").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                return orderEvent.getEventType().equals("create");
            }
        }).followedBy("pay").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                return orderEvent.getEventType().equals("pay");
            }
        }).within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, eventPattern);

        //超时的事件要做报警处理
        OutputTag<OrderResult> timeOutTag=new OutputTag("orderTimeOut",TypeInformation.of(OrderResult.class));
        SingleOutputStreamOperator<OrderResult> outputStream = patternStream.select(timeOutTag, new PatternTimeoutFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                String orderId = map.get("create").iterator().next().getOrderId();
                return new OrderResult(orderId, "timeOut");
            }
        }, new PatternSelectFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
                String orderId = map.get("pay").iterator().next().getOrderId();
                return new OrderResult(orderId, "paySuccess");
            }
        });

        outputStream.print();
        outputStream.getSideOutput(timeOutTag).print("timeOut");

        env.execute(OrderLoseDetectApplication.class.getSimpleName());

    }
}
