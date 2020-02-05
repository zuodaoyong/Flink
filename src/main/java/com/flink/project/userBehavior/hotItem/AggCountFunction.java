package com.flink.project.userBehavior.hotItem;

import com.flink.project.userBehavior.entity.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AggCountFunction implements AggregateFunction<UserBehavior,Long,Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong+1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}
