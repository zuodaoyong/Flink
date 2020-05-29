package com.flink.examples.cep;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LoginCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<LoginEvent> stream = env.fromCollection(Arrays.asList(new LoginEvent[]{
                new LoginEvent(1L, "张三", "fail", 1590677109L),
                new LoginEvent(2L, "张三", "fail", 1590677110L),
                new LoginEvent(3L, "张三", "fail", 1590677119L),
                new LoginEvent(4L, "李四", "fail", 1590677109L),
                new LoginEvent(5L, "李四", "success", 1590677110L),
                new LoginEvent(6L, "张三", "fail", 1590677129L)
        })).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent element) {
                return element.getEventTime()*1000L;
            }
        });
        //定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("login").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                if (loginEvent.eventType.equals("fail")) {
                    return true;
                }
                return false;
            }
        }).next("fail2").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                if (loginEvent.eventType.equals("fail")) {
                    return true;
                }
                return false;
            }
        }).next("fail3").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                if (loginEvent.eventType.equals("fail")) {
                    return true;
                }
                return false;
            }
        }).within(Time.minutes(2));

        PatternStream<LoginEvent> patternStream = CEP.pattern(stream.keyBy(new KeySelector<LoginEvent, String>() {
            @Override
            public String getKey(LoginEvent loginEvent) throws Exception {
                return loginEvent.userName;
            }
        }), pattern);

        patternStream.select(new PatternSelectFunction<LoginEvent,String>(){

            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                Iterator<Map.Entry<String, List<LoginEvent>>> iterator = map.entrySet().iterator();
                String key="";
                while (iterator.hasNext()){
                    Map.Entry<String, List<LoginEvent>> next = iterator.next();
                    key=next.getKey();
                    System.out.println(next.getValue());
                }
                return key;
            }
        }).print();

        env.execute(LoginCEP.class.getSimpleName());
    }

    static class LoginEvent{
        public LoginEvent(Long id, String userName, String eventType, Long eventTime) {
            this.id = id;
            this.userName = userName;
            this.eventType = eventType;
            this.eventTime = eventTime;
        }

        private Long id;
        private String userName;
        private String eventType;
        private Long eventTime;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public Long getEventTime() {
            return eventTime;
        }

        public void setEventTime(Long eventTime) {
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return "LoginEvent{" +
                    "id=" + id +
                    ", userName='" + userName + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }
}
