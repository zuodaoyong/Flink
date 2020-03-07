package com.flink.project.userBehavior.HotPage;

import com.common.TimeUtils;
import com.flink.project.userBehavior.entity.ApacheLog;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HotPageFlatMapFunction implements FlatMapFunction<String, ApacheLog> {

    @Override
    public void flatMap(String msg, Collector<ApacheLog> collector) throws Exception {
        String[] split = msg.split("\\s");
        if(split.length==7){
            ApacheLog apacheLog=new ApacheLog();
            apacheLog.setIp(split[0]);
            apacheLog.setUserId(split[2]);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            apacheLog.setEventTime(simpleDateFormat.parse(split[3]).getTime());
            apacheLog.setMethod(split[5]);
            apacheLog.setUrl(split[6]);
            collector.collect(apacheLog);
        }
    }
}
