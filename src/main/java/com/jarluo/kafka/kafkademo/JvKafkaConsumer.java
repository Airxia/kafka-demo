package com.jarluo.kafka.kafkademo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @from: https://www.cnblogs.com/java333/
 * @desc: TODO
 * @author: jar luo
 * @date: 2019/8/19 19:32
 */
public class JvKafkaConsumer extends Thread{
    KafkaConsumer<Integer,String> consumer;
    String topic;
    public JvKafkaConsumer(String topic){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.11:9092,192.168.1.12:9092,192.168.1.13:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"Jv-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Jv-groupId");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"30000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        consumer = new KafkaConsumer<Integer, String>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singleton(this.topic));
        while (true){
            ConsumerRecords<Integer, String> consumerRecords =consumer.poll(Duration.ofSeconds(1));
            consumerRecords.forEach(record->{
                System.out.println(record.key()+"->"+record.value()+"->"+record.offset());
            });
        }
    }

    public static void main(String[] args) {
        new JvKafkaConsumer("test").start();
    }

}
