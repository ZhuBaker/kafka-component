package com.lh.kafka.component.queue;

import java.util.Map;

import com.lh.kafka.component.queue.exception.MQException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午2:25:20
 * 说明：消费接口
 */
public interface MQConsumer<K, V> {

    /**
     * 处理消息
     * @param message
     * @throws MQException
     */
    public void handle(V message) throws MQException;

    /**
     * 处理消息
     * @param key
     * @param message
     * @throws MQException
     */
    public void handle(K key, V message) throws MQException;

    /**
     * 处理消息
     * @param message
     * @throws MQException
     */
    public void handle(Map<K, V> message) throws MQException;
}
