package com.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
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
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<100;i++){
            //producer.send(new ProducerRecord<String, String>("test",Integer.toString(i),Integer.toString(i)));
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "haha@"+Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println(recordMetadata.partition()+"@"+recordMetadata.offset());
                    }else{
                        System.out.println(e);
                    }
                }
            });
            //System.out.println("发送消息");
        }
        producer.close();
    }
}
