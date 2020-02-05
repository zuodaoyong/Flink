package com.flink.project.userBehavior.hotItem;

import com.common.TimeUtils;
import com.flink.project.userBehavior.entity.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class HotItemProcessFunction extends KeyedProcessFunction<Long,ItemViewCount,String> {

    private ListState<ItemViewCount> itemState;
    private Integer limit;
    public HotItemProcessFunction(Integer limit){
        this.limit=limit;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        itemState=getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemState",ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
        itemState.add(itemViewCount);
        //注册一个定时器
        context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd()+1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Iterable<ItemViewCount> itemViewCounts = itemState.get();
        itemState.clear();
        Set<ItemViewCount> sets=new TreeSet<ItemViewCount>();
        Iterator<ItemViewCount> iterator = itemViewCounts.iterator();
        while (iterator.hasNext()){
            sets.add(iterator.next());
        }
        int i=1;
        StringBuffer buffer=new StringBuffer("");
        buffer.append("时间="+TimeUtils.parseToFormatTime(new Date(timestamp-1),TimeUtils.SECONDSTR)).append("\n");
        Thread.sleep(1000);
        for(ItemViewCount item:sets){
            if(i>this.limit){
                break;
            }
            buffer.append("商品ID=").append(item.getItemId()).append("，");
            buffer.append("count=").append(item.getCount()).append("\n");
            i++;
        }
        out.collect(buffer.toString());
    }
}
