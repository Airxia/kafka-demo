package com.jarluo.kafka.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @from: https://www.cnblogs.com/java333/
 * @desc: TODO
 * @author: jar luo
 * @date: 2019/8/17 20:28
 */
public class JvKafkaProducer extends Thread {
    KafkaProducer<Integer,String> producer;
    /**
     * 主题
     */
    String topic;
    public JvKafkaProducer(String topic){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.11:9092,192.168.1.12:9092,192.168.1.13:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"jv-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //连接字符串
        //通过工厂
        //通过new
        producer = new KafkaProducer<Integer, String>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        int num = 0;
        String msg = "Jv kafka demo msg:"+num;
        while(num < 20){
            try {
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic,msg)).get();
                System.out.println(recordMetadata.offset()+"->"+recordMetadata.partition()+"->"+recordMetadata.topic());
                TimeUnit.SECONDS.sleep(2);
                ++num;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new JvKafkaProducer("test").start();
    }
}
