package com.flink.project.userBehavior.hotItem;

import com.flink.project.userBehavior.entity.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;

public class HotItemMapFunction implements MapFunction<String, UserBehavior> {

    @Override
    public UserBehavior map(String input) throws Exception {
        String[] split = input.split(",");
        UserBehavior userBehavior=new UserBehavior();
        userBehavior.setUserId(Long.valueOf(split[0]));
        userBehavior.setItemId(Long.valueOf(split[1]));
        userBehavior.setCategoryId(Integer.valueOf(split[2]));
        userBehavior.setBehavior(split[3]);
        userBehavior.setTimestramp(Long.valueOf(split[4]));
        return userBehavior;
    }
}
