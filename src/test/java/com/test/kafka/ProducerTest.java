package com.test.kafka;

import com.kafka.examples.Producer;

public class ProducerTest {

    public static void main(String[] args) {
        new Producer("test",true).start();
    }
}
