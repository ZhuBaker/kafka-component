package com.lh.kafka.component.queue;

import java.util.Map;

import com.lh.kafka.component.queue.exception.MQException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月10日 下午3:13:47
 * 说明：消费(手动消费)接口
 */
public interface MQNoAutoConsumer<K, V, E> {

    /**
     * 处理消息
     * @param message   消息对象
     * @param extend   扩展对象
     * @throws MQException
     */
    public void handle(V message, E extend) throws MQException;

    /**
     * 处理消息
     * @param key   消息对象key
     * @param message   消息对象
     * @param extend   扩展对象
     * @throws MQException
     */
    public void handle(K key, V message, E extend) throws MQException;

    /**
     * 处理消息
     * @param message   消息对象
     * @param extend   扩展对象
     * @throws MQException
     */
    public void handle(Map<K, V> message, E extend) throws MQException;
}
