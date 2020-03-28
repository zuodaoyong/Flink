package com.flink.examples.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StreamDataSourceB extends RichParallelSourceFunction<Tuple3<String,String,Long>> {

    private volatile boolean running=true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> sourceContext) throws Exception {

        Tuple3[] elements=new Tuple3[]{
                Tuple3.of("a","hangzhou",1000000059000L),  //[50000-60000)
                Tuple3.of("b","beijing", 1000000105000L)   //{10000-11000)
        };

        int count=0;
        while (running&&count<elements.length){
            sourceContext.collect(new Tuple3<>((String) elements[count].f0,(String) elements[count].f1,(Long) elements[count].f2));
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
