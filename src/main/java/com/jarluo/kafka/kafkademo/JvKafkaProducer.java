package com.jarluo.kafka.kafkademo;

import org.apache.kafka.clients.producer.*;
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
        //kafka默认是批量发送
//        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"100");
        //涉及到两次间隔时间 linger.ms 谁先满足就先执行谁
//        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1000");
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
                //get 会拿到发送的结果
                //同步 get()->Future() 带阻塞
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic,msg)).get();
                //异步 带回调
//                RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, msg), (recordMetadata1, e) -> {
//
//                }).get();

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
