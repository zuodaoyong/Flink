package com.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class CustomConsumer {

    public static void main(String[] args) {
        manualASyncSubmitOffset();
    }

    //手动异步提交offset
    private static void manualASyncSubmitOffset(){
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        //properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "a");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(new String[]{"bus"}));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record:records){
                System.out.println(record.partition()+"@"+record.offset()+"，key="+record.key()+"，value="+record.value());
            }
            //异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("Commit failed for" + offsets);
                    }else{
                        System.err.println("Commit for" + offsets);
                    }
                }
            });
        }
    }

    //手动同步提交offset
    private static void manualSyncSubmitOffset(){
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        //properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "a");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(new String[]{"bus"}));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record:records){
                System.out.println(record.partition()+"@"+record.offset()+"，key="+record.key()+"，value="+record.value());
            }
            //同步提交，当前线程会阻塞直到 offset 提交成功
            consumer.commitSync();
        }
    }
    //自动提交offset
    private static void autoSubmitOffset(){
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "a");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(new String[]{"bus"}));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record:records){
                System.out.println(record.partition()+"@"+record.offset()+"，key="+record.key()+"，value="+record.value());
            }
        }
    }
}
