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
    
    private KafkaMessageDecoder<K, V> messageDecoder = new KafkaMessageDecoder<K, V>();
    
    private MQConsumer<K, V> mqConsumer;

    private KafkaTopic kafkaTopic;

    private KafkaMessageAdapter() {
        super();
    }

    public KafkaMessageAdapter(MQConsumer<K, V> mqConsumer,
            KafkaTopic kafkaTopic) {
        this();
        this.mqConsumer = mqConsumer;
        this.kafkaTopic = kafkaTopic;

        if(kafkaTopic == null){
            throw new IllegalArgumentException("Property param [kafkaTopic] must be not null.");
        }
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
    public void adapter(ConsumerRecords<?, ?> records) throws MQException{
        if(this.mqConsumer == null){
            throw new MQException("MessageAdapter property [mqConsumer] is null.");
        }
        
        Map<byte[], byte[]> messageBytes = new HashMap<byte[], byte[]>();
        for (ConsumerRecord<?, ?> record : records) {
            messageBytes.put((byte[])record.key(), (byte[])record.value());
        }
        
        Map<K, V> message = messageDecoder.decodeMap(messageBytes);
        mqConsumer.handle(message);
    }
    
    /**
     * 消息适配
     * @param record
     * @throws MQException
     */
    public void adapter(ConsumerRecord<?, ?> record) throws MQException{
        if(this.mqConsumer == null){
            throw new MQException("MessageAdapter property [mqConsumer] is null.");
        }
        K key = messageDecoder.decodeKey((byte[]) record.key());
        V message = messageDecoder.decodeVal((byte[]) record.value());
        mqConsumer.handle(key, message);
    }
}
