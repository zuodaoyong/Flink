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
        properties.put("zookeeper.connect","spark1:2181");//zk��Ⱥ��ַ
        properties.put("group.id","test_kafka_group");//��������
        properties.put("zookeeper.session.timeout.ms","4000");//zk�Ự��ʱʱ��
        properties.put("zookeeper.sync.time.ms","2000");//zk follower���leader��ʱ��
        properties.put("auto.commit.interval.ms","1000");//�Զ��ύƫ�����ļ��
        ConsumerConfig config=new ConsumerConfig(properties);
        singleThreadConsumer("test",config);

    }

    /**
     * һ���������߳�
     */
    public static void singleThreadConsumer(String topic,ConsumerConfig config){
        Map<String,Integer> topicCountMap=new HashMap<>();//����ÿ��������߳�����
        topicCountMap.put(topic, new Integer(1));
        //������������������������߶�����Ϣ����kafkaStream
        ConsumerConnector connector= Consumer.createJavaConsumerConnector(config);
        //ÿ���������̶߳���Ӧ��һ����Ϣ��,��Ϣ�������Ϣ��������������
        Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap=connector.createMessageStreams(topicCountMap);
        //�����ߵ�����������Ϣ���ж�ȡ��Ϣ
        List<KafkaStream<byte[],byte[]>> streams=consumerMap.get(topic);
        KafkaStream<byte[],byte[]> stream=streams.get(0);//һ���̣߳����ֻ��һ����Ϣ��
        ConsumerIterator<byte[],byte[]> it = stream.iterator();
        while(it.hasNext()){
            System.out.println("receive,message:"+new String(it.next().message()));
        }
    }
}