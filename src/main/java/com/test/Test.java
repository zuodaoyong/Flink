package com.test;


import com.flink.project.loginFail.entity.LoginEvent;
import com.flink.project.loginFail.entity.LoginWarn;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("D:\\temp\\flink\\data\\LoginLog.csv");
        DataStream<LoginEvent> dataStream =streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new LoginEvent(Long.valueOf(split[0]),split[1],split[2],Long.valueOf(split[3]));
            }
        });

        KeyedStream<LoginEvent, Long> keyedStream = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.milliseconds(10)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getEventTime() * 1000;
            }
        }).keyBy(new KeySelector<LoginEvent, Long>() {
            @Override
            public Long getKey(LoginEvent loginEvent) throws Exception {
                return loginEvent.getUserId();
            }
        });

        Pattern<LoginEvent, LoginEvent> eventPattern = Pattern.<LoginEvent>begin("start").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return loginEvent.getEventType().equals("fail");
            }
        }).next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return loginEvent.getEventType().equals("fail");
            }
        }).within(Time.seconds(5));

        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, eventPattern);

        patternStream.select(new PatternSelectFunction<LoginEvent, LoginWarn>() {
            @Override
            public LoginWarn select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent startLoginEvent = map.get("start").iterator().next();
                LoginEvent lastLoginEvent = map.get("next").iterator().next();
                return new LoginWarn(startLoginEvent.getUserId(),startLoginEvent.getEventTime(),lastLoginEvent.getEventTime(),"fail login >=2");
            }
        }).print();

        env.execute(Test.class.getSimpleName());
    }
}

