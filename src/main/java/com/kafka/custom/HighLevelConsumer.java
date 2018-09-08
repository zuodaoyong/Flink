package com.kafka.custom;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HighLevelConsumer {

    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put("zookeeper.connect","spark1:2181");//zk集群地址
        properties.put("group.id","test_kafka_group");//消费组名
        properties.put("zookeeper.session.timeout.ms","4000");//zk会话超时时间
        properties.put("zookeeper.sync.time.ms","2000");//zk follower落后leader的时间
        properties.put("auto.commit.interval.ms","1000");//自动提交偏移量的间隔
        ConsumerConfig config=new ConsumerConfig(properties);
        singleThreadConsumer("test",config);

    }

    /**
     * 一个消费者线程
     */
    public static void singleThreadConsumer(String topic,ConsumerConfig config){
        Map<String,Integer> topicCountMap=new HashMap<>();//设置每个主题的线程数量
        topicCountMap.put(topic, new Integer(1));
        //消费者连接器，会根据消费者订阅信息创建kafkaStream
        ConsumerConnector connector= Consumer.createJavaConsumerConnector(config);
        //每个消费者线程都对应了一个消息流,消息会放入消息流的阻塞队列中
        Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap=connector.createMessageStreams(topicCountMap);
        //消费者迭代器，从消息流中读取消息
        List<KafkaStream<byte[],byte[]>> streams=consumerMap.get(topic);
        KafkaStream<byte[],byte[]> stream=streams.get(0);//一个线程，因此只有一个消息流
        ConsumerIterator<byte[],byte[]> it = stream.iterator();
        while(it.hasNext()){
            System.out.println("receive,message:"+new String(it.next().message()));
        }
    }
}
