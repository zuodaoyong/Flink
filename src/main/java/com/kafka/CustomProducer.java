package com.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CustomProducer {
    public static void main(String[] args) {
        asyncApiWithOutReturn();
    }

    //不带返回值的异步API
    private static void asyncApiWithOutReturn(){
        Properties properties=new Properties();
        //kafka 集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        //批次大小（16k）
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        ///RecordAccumulator 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafka.CustomPartitioner");
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++){
            producer.send(new ProducerRecord<String, String>("bus",Integer.toString(i),"busv4@"+Integer.toString(i)));
        }
        producer.close();
    }

    //带返回值的异步API
    private static void asyncApiWithReturn(){
        Properties properties=new Properties();
        //kafka 集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        //批次大小（16k）
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        ///RecordAccumulator 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafka.CustomPartitioner");
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10000;i++){
            producer.send(new ProducerRecord<String, String>("bus", Integer.toString(i), "haha@"+Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println(recordMetadata.partition()+"@"+recordMetadata.offset());
                    }else{
                        System.out.println(e);
                    }
                }
            });
        }
        producer.close();
    }

    //同步API
    private static void syncApi(){
        Properties properties=new Properties();
        //kafka 集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        //批次大小（16k）
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        ///RecordAccumulator 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafka.CustomPartitioner");
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10000;i++){
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
            try {
                RecordMetadata recordMetadata = future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
