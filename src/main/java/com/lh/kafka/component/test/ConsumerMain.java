package com.lh.kafka.component.test;

import com.lh.kafka.component.queue.MQConsumer;
import com.lh.kafka.component.queue.MQNoAutoConsumer;
import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.KafkaNoAutoReceiverMQ;
import com.lh.kafka.component.queue.kafka.KafkaReceiverMQ;
import com.lh.kafka.component.queue.kafka.adapter.KafkaMessageAdapter;
import com.lh.kafka.component.queue.kafka.adapter.KafkaNoAutoMessageAdapter;
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
        
        boolean step2 = true;
        
        if(!step2){
            //step1
            {
                KafkaMessageAdapter<String,String> adater = new KafkaMessageAdapter<String, String>(mqConsumer, kafkaTopic);
                KafkaReceiverMQ receiverMQ = new KafkaReceiverMQ(config,adater);
                receiverMQ.start();
            }
        } else {
            //step2
            {
                KafkaNoAutoMessageAdapter<String,String, Integer> adater = new KafkaNoAutoMessageAdapter<String, String, Integer>(mqNoAutoConsumer, kafkaTopic);
                KafkaNoAutoReceiverMQ<Integer> receiverMQ = new KafkaNoAutoReceiverMQ<Integer>(config, adater);
                Integer extend = 100;
                int num = -1;
                try {
                    num = receiverMQ.receive(5, extend);
                } catch (MQException e) {
                    e.printStackTrace();
                }
                System.out.println("Consumer size=" + num);
            }
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

    static MQNoAutoConsumer<String, String, Integer> mqNoAutoConsumer = new MQNoAutoConsumer<String, String, Integer>() {
        
        @Override
        public void handle(Map<String, String> message, Integer extend)
                throws MQException {
            System.out.println("message=" + message + ",extend=" + extend);
        }
        
        @Override
        public void handle(String key, String message, Integer extend)
                throws MQException {
            System.out.println("key="+ key + ",message=" + message + ",extend=" + extend);
        }
        
        @Override
        public void handle(String message, Integer extend) throws MQException {
            System.out.println("message=" + message + ",extend=" + extend);
        }
    };
}
