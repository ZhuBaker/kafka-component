package com.lh.kafka.component.queue.kafka.support;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月10日 下午4:43:28
 * 说明：本地消费队列元素对象
 */
public class NoAutoConsumerRecordQueueItem<K, V, E> {

    /**
     * 消费对象
     */
    private ConsumerRecord<K, V> consumerRecord;
    
    /**
     * 扩展对象
     */
    private E extend;
    
    public NoAutoConsumerRecordQueueItem(ConsumerRecord<K, V> consumerRecord) {
        super();
        this.consumerRecord = consumerRecord;
    }

    public NoAutoConsumerRecordQueueItem(ConsumerRecord<K, V> consumerRecord,
            E extend) {
        super();
        this.consumerRecord = consumerRecord;
        this.extend = extend;
    }
    
    public ConsumerRecord<K, V> getConsumerRecord() {
        return consumerRecord;
    }

    public void setConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    public E getExtend() {
        return extend;
    }

    public void setExtend(E extend) {
        this.extend = extend;
    }
}
