package com.flink.project.userBehavior.HotPage;

import com.common.TimeUtils;
import com.flink.project.userBehavior.entity.HotPageViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class HotPageProcessFunction extends KeyedProcessFunction<Long, HotPageViewCount,String> {

    private ListState<HotPageViewCount> hotPageViewCountListState=null;

    @Override
    public void open(Configuration parameters) throws Exception {
        hotPageViewCountListState=getRuntimeContext().getListState(new ListStateDescriptor("hotPageViewCountListState",HotPageViewCount.class));
    }

    @Override
    public void processElement(HotPageViewCount hotPageViewCount, Context context, Collector<String> collector) throws Exception {
        hotPageViewCountListState.add(hotPageViewCount);
        context.timerService().registerEventTimeTimer(hotPageViewCount.getWindowEnd()+1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Set<HotPageViewCount> sets=new TreeSet<HotPageViewCount>();
        Iterator<HotPageViewCount> iterator = hotPageViewCountListState.get().iterator();
        hotPageViewCountListState.clear();
        while (iterator.hasNext()){
            sets.add(iterator.next());
        }
        StringBuffer res=new StringBuffer("");
        res.append("time=").append(TimeUtils.parseToFormatTime(new Date(timestamp-1),TimeUtils.SECONDSTR));
        /*for(HotPageViewCount hotPageViewCount:sets){
            res.append(",url=").append(hotPageViewCount.getUrl())
                    .append(",count=").append(hotPageViewCount.getCount()).append("\n");
        }*/
        out.collect(res.toString());
    }
}
