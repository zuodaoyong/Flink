package com.flink.examples.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;

public class FileSourceWithState extends RichParallelSourceFunction<Tuple2<String,String>> implements CheckpointedFunction {

    private String path;
    public FileSourceWithState(String path){
        this.path=path;
    }

    private transient ListState<Long> offsetListState;
    private boolean isRunning=true;
    private Long offset=0L;
    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        Iterator<Long> iterator = offsetListState.get().iterator();
        while (iterator.hasNext()){
            offset=iterator.next();
        }
        //获取当前subTask
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        String fileName=path + "/" + subtask + ".txt";
        RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
        //从指定的offset读取文件内容
        randomAccessFile.seek(offset);
        final Object checkpointLock = sourceContext.getCheckpointLock();
        while (isRunning){
            String line = randomAccessFile.readLine();
            if(line!=null){
                String message = new String(line.getBytes("ISO-8859-1"), "utf-8");
                synchronized (checkpointLock){//与snapshotState方法共享offset，会有线程安全问题，所以要加锁
                    offset = randomAccessFile.getFilePointer();
                    sourceContext.collect(new Tuple2<String, String>(fileName,message));
                }
            }else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }

    /**
     * 定期将state保存到statebackend中
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //清除历史数据
        offsetListState.clear();
        //更新最新值
        offsetListState.update(Collections.singletonList(offset));
    }

    /**
     * 只执行一次
     * @param functionInitializationContext
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        //初始化或者获取历史状态
        //getRuntimeContext()获取的state是keyState，如何获取opratorState?
        ListStateDescriptor<Long> offsetListStateDescriptor=new ListStateDescriptor<Long>("offset-state", TypeInformation.of(new TypeHint<Long>() {
        }));
        offsetListState = functionInitializationContext.getOperatorStateStore().getListState(offsetListStateDescriptor);
    }
}
