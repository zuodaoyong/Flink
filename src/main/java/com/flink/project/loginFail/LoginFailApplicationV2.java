package com.flink.project.loginFail;

import com.flink.project.loginFail.entity.LoginEvent;
import com.flink.project.loginFail.entity.LoginWarn;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoginFailApplicationV2 {

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
        }).process(new LoginWarnProcessFunctionv2()).print();

        env.execute(LoginFailApplication.class.getSimpleName());
    }
}

class LoginWarnProcessFunctionv2 extends KeyedProcessFunction<Long,LoginEvent, LoginWarn> {


    private ValueState<LoginWarn> loginWarnState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginWarnState=getRuntimeContext().getState(new ValueStateDescriptor<LoginWarn>("loginWarnState", LoginWarn.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginWarn> out) throws Exception {
        LoginWarn valueState = loginWarnState.value();
        if(value.getEventType().equals("fail")){
            if(valueState==null){
                valueState=new LoginWarn(ctx.getCurrentKey(),value.getEventTime(),null,"fail login >=2");
                loginWarnState.update(valueState);
            }else{
                valueState.setLastFailTime(value.getEventTime());
            }
            if(valueState.getFirstFailTime()!=null&&valueState.getLastFailTime()!=null){
                if(valueState.getLastFailTime()-valueState.getFirstFailTime()>=2L){
                    out.collect(valueState);
                    loginWarnState.clear();
                }
            }
        } else {
            loginWarnState.clear();
        }
    }


}