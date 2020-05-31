package com.flink.project.loginFail;

import com.flink.project.loginFail.entity.LoginEvent;
import com.flink.project.loginFail.entity.LoginWarn;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoginFailApplication {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> streamSource = env.readTextFile("D:\\temp\\flink\\data\\LoginLog.csv");
        streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new LoginEvent(Long.valueOf(split[0]),split[1],split[2],Long.valueOf(split[3]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getEventTime()*1000;
            }
        }).keyBy(new KeySelector<LoginEvent, Long>() {
            @Override
            public Long getKey(LoginEvent loginEvent) throws Exception {
                return loginEvent.getUserId();
            }
        }).process(new LoginWarnProcessFunction(2)).print();

        env.execute(LoginFailApplication.class.getSimpleName());
    }
}

class LoginWarnProcessFunction extends KeyedProcessFunction<Long,LoginEvent, LoginWarn> {


    private Integer failNum;
    public LoginWarnProcessFunction(Integer failNum){
        this.failNum=failNum;
    }
    private ListState<LoginEvent> loginEventListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginEventListState=getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginEventListState", LoginEvent.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginWarn> out) throws Exception {
        Iterable<LoginEvent> loginEventsIterable = loginEventListState.get();
        if(value.getEventType().equals("fail")){
            if(!loginEventsIterable.iterator().hasNext()){
                ctx.timerService().registerEventTimeTimer(value.getEventTime()*1000L+2000L);
            }
            loginEventListState.add(value);
        } else {
            loginEventListState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginWarn> out) throws Exception {
        Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
        List<LoginEvent> loginEventList=new ArrayList<>();
        while (iterator.hasNext()){
            LoginEvent next = iterator.next();
            loginEventList.add(next);
        }
        if(loginEventList.size()>=this.failNum){
            out.collect(new LoginWarn(ctx.getCurrentKey(),loginEventList.get(0).getEventTime(),loginEventList.get(loginEventList.size()-1).getEventTime(),"fail login num is "+loginEventList.size()));
        }
        loginEventListState.clear();
    }
}