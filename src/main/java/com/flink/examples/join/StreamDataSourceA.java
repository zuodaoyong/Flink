package com.flink.examples.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StreamDataSourceA extends RichParallelSourceFunction<Tuple3<String, String,Long>> {

    private volatile boolean running=true;
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> sourceContext) throws Exception {

        Tuple3[] elements=new Tuple3[]{
                Tuple3.of("a","1",1000000050000L),  //[50000-60000)
                Tuple3.of("a","1",1000000054000L),  //[50000-60000)
                Tuple3.of("a","1",1000000079900L),  //[70000-80000)
                Tuple3.of("a","1",1000000115000L),  //[110000-120000)
                Tuple3.of("b","1",1000000100000L),  //[100000-110000)
                Tuple3.of("b","1",1000000108000L)   //[100000-110000)
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
