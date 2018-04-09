package com.lh.kafka.component.test;

import com.lh.kafka.component.queue.MQConsumer;
import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.KafkaReceiverMQ;
import com.lh.kafka.component.queue.kafka.adapter.KafkaMessageAdapter;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import java.util.Map;

/**
 * Created by Linhao on 2018/4/6.
 */
public class ConsumerMain {

    public static void main(String[] args){

        Resource config = new DefaultResourceLoader().getResource("kafka/kafka-consumer.properties");
        KafkaTopic kafkaTopic = new KafkaTopic("my-test-topic");
        
        //step1
        {
            KafkaMessageAdapter<String,String> adater = new KafkaMessageAdapter<String, String>(mqConsumer, kafkaTopic);
            KafkaReceiverMQ<byte[],byte[]> receiverMQ = new KafkaReceiverMQ<byte[], byte[]>(config,adater);
            receiverMQ.start();
        }
        

        //step2
        {
            
        }

        System.out.println("Consumer start success.");
        try {
            Thread.sleep(24 * 3600 * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    static MQConsumer<String,String> mqConsumer = new MQConsumer<String,String>(){

        @Override
        public void handle(String message) throws MQException {
            System.out.println("message=" + message);
        }

        @Override
        public void handle(String key, String message) throws MQException {
            System.out.println("key="+ key + ",message=" + message);
        }

        @Override
        public void handle(Map<String, String> message) throws MQException {
            System.out.println("message=" + message);
        }
    };
}
