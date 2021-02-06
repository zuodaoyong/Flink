package com.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTest {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "121.239.169.29:9092");
        //props.put("zk.connect", "121.239.169.29:2181");
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("advertised.host.name", "121.239.169.29");
//        props.put("advertised.port", "9092");
//        props.put("topic.metadata.refresh.interval.ms", 120000);
//        props.put("metadata.broker.list", "121.239.169.29:9092");

        //生产者发送消息
        String topic = "testtest";
        Producer<String, String> procuder = new KafkaProducer<String, String>(props);
        String data="xxxx";
        procuder.send(new ProducerRecord<String, String>(topic,data));

        System.out.println(data);
        //System.out.println(recordMetadata.offset());
    }
}
