package com.lh.kafka.component.queue.kafka.client.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;

import com.lh.kafka.component.queue.kafka.IKafkaCallback;
import com.lh.kafka.component.queue.kafka.exception.KafkaUnrecoverableException;


/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 下午3:31:18
 * 说明：kafka消息生产者
 */
public interface IKafkaMsSenderClient<K, V> extends Producer<K, V> {

    /**
     * 发送数据
     * @param topic 指定topic主题
     * @param value 发送得数据
     * @throws KafkaUnrecoverableException
     * @throws KafkaException
     */
    void send(String topic, V value) throws KafkaUnrecoverableException, KafkaException;

    /**
     * 发送数据
     * @param topic 指定topic主题
     * @param value 数据
     * @param callback  发送回调
     */
    void send(String topic, V value, IKafkaCallback callback);
    
    /**
     * 发送数据
     * @param topic 指定topic主题
     * @param key   key值
     * @param value 数据
     * @throws KafkaUnrecoverableException
     * @throws KafkaException
     */
    void sendMap(String topic, K key, V value) throws KafkaUnrecoverableException, KafkaException;

    /**
     * 发送数据
     * @param topic 指定topic主题
     * @param key   key值
     * @param value 数据
     * @param callback  发送回调
     */
    void sendMap(String topic, K key, V value, IKafkaCallback callback);
    
    /**
     * 设置事务是否自动提交
     * @param autoCommitTransaction
     */
     void setAutoCommitTransaction(boolean autoCommitTransaction);

     /**
      * 取得事务是否自动提交
      * @return
      */
     boolean getAutoCommitTransaction();
     
     /**
      * 开始事务
      */
     void beginTransaction();
     
     /**
      * 提交事务
      */
     void commitTransaction();

     /**
      * 停止生产数据
      */
     void shutDown();
     
     /**
      * 事务回滚
      */
     void rollback();
}
