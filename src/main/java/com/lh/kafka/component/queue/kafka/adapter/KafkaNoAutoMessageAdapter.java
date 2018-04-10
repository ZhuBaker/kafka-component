package com.lh.kafka.component.queue.kafka.adapter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.lh.kafka.component.queue.MQNoAutoConsumer;
import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.codec.KafkaMessageDecoder;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月10日 下午2:38:08
 * 说明：消息适配器
 */
public class KafkaNoAutoMessageAdapter<K extends Serializable, V extends Serializable, E> {
    
    private KafkaMessageDecoder<K, V> messageDecoder = new KafkaMessageDecoder<K, V>();
    
    private MQNoAutoConsumer<K, V, E> mqNoAutoConsumer;

    private KafkaTopic kafkaTopic;
    
    private KafkaNoAutoMessageAdapter() {
        super();
    }

    public KafkaNoAutoMessageAdapter(MQNoAutoConsumer<K, V, E> mqNoAutoConsumer,
            KafkaTopic kafkaTopic) {
        this();
        this.mqNoAutoConsumer = mqNoAutoConsumer;
        this.kafkaTopic = kafkaTopic;

        if(kafkaTopic == null){
            throw new IllegalArgumentException("Property param [kafkaTopic] must be not null.");
        }
    }

    public MQNoAutoConsumer<K, V, E> getMqNoAutoConsumer() {
        return mqNoAutoConsumer;
    }

    public void setMqNoAutoConsumer(MQNoAutoConsumer<K, V, E> mqNoAutoConsumer) {
        this.mqNoAutoConsumer = mqNoAutoConsumer;
    }

    public KafkaTopic getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(KafkaTopic kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    /**
     * 消息适配
     * @param message   消息对象
     * @param extend   扩展对象
     * @throws MQException
     */
    public void adapter(ConsumerRecords<?, ?> records, E extend) throws MQException{
        if(this.mqNoAutoConsumer == null){
            throw new MQException("MessageAdapter property [mqNoAutoConsumer] is null.");
        }
        
        Map<byte[], byte[]> messageBytes = new HashMap<byte[], byte[]>();
        for (ConsumerRecord<?, ?> record : records) {
            messageBytes.put((byte[])record.key(), (byte[])record.value());
        }
        
        Map<K, V> message = messageDecoder.decodeMap(messageBytes);
        mqNoAutoConsumer.handle(message, extend);
    }
    
    /**
     * 消息适配
     * @param message   消息对象
     * @param extend   扩展对象
     * @throws MQException
     */
    public void adapter(ConsumerRecord<?, ?> record, E extend) throws MQException{
        if(this.mqNoAutoConsumer == null){
            throw new MQException("MessageAdapter property [mqConsumer] is null.");
        }
        K key = messageDecoder.decodeKey((byte[]) record.key());
        V message = messageDecoder.decodeVal((byte[]) record.value());
        mqNoAutoConsumer.handle(key, message, extend);
    }
}
