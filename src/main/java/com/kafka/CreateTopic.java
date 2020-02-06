package com.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Properties;

public class CreateTopic {

    public static void main(String[] args) {
        ZkUtils zkUtils = ZkUtils.
                apply("master:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());

        /*AdminUtils.createTopic(zkUtils, "javaCreateTestTopic",  2,
                2,  new Properties(), new RackAwareMode.Enforced$());*/
        zkUtils.close();
    }
}
