package com.flink.examples.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

public class FileSourceNotState extends RichParallelSourceFunction<Tuple2<String,String>> {


    private String path;
    public FileSourceNotState(String path){
        this.path=path;
    }

    private boolean isRunning=true;

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        //获取当前subTask
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        String fileName=path + "/" + subtask + ".txt";
        RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
        while (isRunning){
            String line = randomAccessFile.readLine();
            if(line!=null){
                String message = new String(line.getBytes("ISO-8859-1"), "utf-8");
                sourceContext.collect(new Tuple2<String, String>(fileName,message));
            }else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
