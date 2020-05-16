package com.kafka.adminclient;


import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;

public class CreateTopic {
    public static void main(String[] args) throws Exception{

        String bootstrapServers="master:9092,slave1:9092";
        String topic="adminclient_test";
        Properties properties=new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,100000);
        AdminClient adminClient = AdminClient.create(properties);
        showConfig(adminClient,topic);
        adminClient.close();
    }

    /**
     * 查询topic的配置
     * @param adminClient
     * @param topic
     * @throws Exception
     */
    private static void showConfig(AdminClient adminClient,String topic) throws Exception{
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(configResource));
        Config config = describeConfigsResult.all().get().get(configResource);
        System.out.println(config);
    }


    /**
     * 创建topic
     * @param adminClient
     * @param topic
     */
    private static void createTopic(AdminClient adminClient,String topic){
        NewTopic newTopic = new NewTopic(topic, 3, (short) 1);
        CreateTopicsResult topicsResult = adminClient.createTopics(Collections.singleton(newTopic));
        topicsResult.all().whenComplete((x,y)->{
            System.out.println("adminclient_test 创建完成");
            System.out.println(x+":"+y);
        });
        adminClient.close();
    }
}
