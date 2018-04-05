package com.lh.kafka.component.queue.kafka.adapter;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.lh.kafka.component.queue.MQConsumer;
import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.codec.KafkaMessageDecoder;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午11:24:26
 * 说明：消息适配器
 */
public class KafkaMessageAdapter<K, V> {
    
    private KafkaMessageDecoder<K, V> decoder;
    
    private MQConsumer<K, V> mqConsumer;

    private KafkaTopic kafkaTopic;
    
    public KafkaMessageAdapter(MQConsumer<K, V> mqConsumer) {
        super();
        this.mqConsumer = mqConsumer;
    }

    public KafkaMessageAdapter(MQConsumer<K, V> mqConsumer,
            KafkaTopic kafkaTopic) {
        super();
        this.mqConsumer = mqConsumer;
        this.kafkaTopic = kafkaTopic;
    }

    public KafkaMessageDecoder<K, V> getDecoder() {
        return decoder;
    }

    public void setDecoder(KafkaMessageDecoder<K, V> decoder) {
        this.decoder = decoder;
    }

    public MQConsumer<K, V> getMqConsumer() {
        return mqConsumer;
    }

    public void setMqConsumer(MQConsumer<K, V> mqConsumer) {
        this.mqConsumer = mqConsumer;
    }

    public KafkaTopic getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(KafkaTopic kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }
    
    /**
     * 消息适配
     * @param records
     * @throws MQException
     */
    public void adapter(ConsumerRecords<K, V> records) throws MQException{
        if(this.mqConsumer == null){
            throw new MQException("MessageAdapter property [mqConsumer] is null.");
        }
        
        Map<K, V> message = new HashMap<K, V>();
        for (ConsumerRecord<K, V> record : records) {
            message.put(record.key(), record.value());
        }
        mqConsumer.handle(message);
    }
    
    /**
     * 消息适配
     * @param record
     * @throws MQException
     */
    public void adapter(ConsumerRecord<K, V> record) throws MQException{
        if(this.mqConsumer == null){
            throw new MQException("MessageAdapter property [mqConsumer] is null.");
        }
        K key = record.key();
        V message = record.value();
        mqConsumer.handle(key, message);
    }
}
