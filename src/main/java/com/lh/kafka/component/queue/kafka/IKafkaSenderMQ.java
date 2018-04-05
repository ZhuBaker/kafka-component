package com.lh.kafka.component.queue.kafka;

import org.apache.kafka.common.KafkaException;

import com.lh.kafka.component.queue.kafka.exception.KafkaUnrecoverableException;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午10:41:56
 * 说明：kafka数据生产接口
 */
public interface IKafkaSenderMQ<K, V> extends IKafakaMQ {
    
    /**
     * 提交事务
     */
    public void commitTransaction();
    
    /**
     * 开启事务
     */
    public void beginTransaction();
    
    /**
     * 事务回滚
     */
    public void rollback();

    /**
     * 销毁
     */
    public void destroy();

    /**
     * 设置事务是否自动提交
     * @param autoCommitTransaction
     */
    public void setAutoCommitTransaction(boolean autoCommitTransaction);
    
    /**
     * 获取事务是否自动提交
     * @return boolean
     */
    public boolean getAutoCommitTransaction();
    
    /**
     * 发送到kafka
     * @param topic
     * @param message
     * @throws KafkaException
     * @throws KafkaUnrecoverableException
     */
    public void send(KafkaTopic topic, V message) throws KafkaException, KafkaUnrecoverableException;
    
    /**
     * 发送到kafka
     * @param topic
     * @param key
     * @param message
     * @throws KafkaException
     * @throws KafkaUnrecoverableException
     */
    public void send(KafkaTopic topic, K key, V message) throws KafkaException, KafkaUnrecoverableException;
    
    /**
     * 发送到kafka
     * @param topic
     * @param message
     * @param callback
     */
    public void send(KafkaTopic topic, V message, IKafkaCallback callback);
    
    /**
     * 发送到kafka
     * @param topic
     * @param key
     * @param message
     * @param callback
     */
    public void send(KafkaTopic topic, K key, V message, IKafkaCallback callback);
}
