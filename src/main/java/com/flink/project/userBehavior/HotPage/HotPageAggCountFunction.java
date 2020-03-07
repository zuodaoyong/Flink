package com.flink.project.userBehavior.HotPage;

import com.flink.project.userBehavior.entity.ApacheLog;
import org.apache.flink.api.common.functions.AggregateFunction;

public class HotPageAggCountFunction implements AggregateFunction<ApacheLog,Integer,Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(ApacheLog apacheLog, Integer integer) {
        return integer+1;
    }

    @Override
    public Integer getResult(Integer integer) {
        return integer;
    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return integer+acc1;
    }
}
