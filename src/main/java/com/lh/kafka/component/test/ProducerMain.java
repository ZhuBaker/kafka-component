package com.lh.kafka.component.test;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.KafkaSenderMQ;
import com.lh.kafka.component.queue.kafka.exception.KafkaUnrecoverableException;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;

import org.apache.kafka.common.KafkaException;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;


/**
 * Created by Linhao on 2018/4/6.
 */
public class ProducerMain {

    public static void main(String[] args){
        KafkaTopic kafkaTopic = new KafkaTopic("my-test-topic");
        Resource config = new DefaultResourceLoader().getResource("kafka/kafka-producer.properties");
        KafkaSenderMQ<String,String> senderMQ = new KafkaSenderMQ<String, String>(config);
        try {
            for (int i = 0; i < 100; i++){
                senderMQ.send(kafkaTopic, "message_" + i);
            }
        } catch (KafkaUnrecoverableException e) {
            e.printStackTrace();
        } catch (KafkaException e) {
            e.printStackTrace();
        } catch (MQException e) {
            e.printStackTrace();
        }
        System.out.println("Producer start success.");
    }
}
