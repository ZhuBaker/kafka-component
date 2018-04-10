package com.lh.kafka.component.queue.kafka;

import com.lh.kafka.component.queue.exception.MQException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午4:42:34
 * 说明：
 */
public interface IKafkaNoAutoReceiverMQ<E> extends IKafakaMQ {

    /**
     * 销毁池
     */
    public void destroy();

    /**
     * 手动消费数据
     * @param num   消费条数
     * @return
     * @throws MQException
     */
    public int receive(int num) throws MQException;

    /**
     * 手动消费数据
     * @param num   消费条数
     * @param extend   扩展对象
     * @return
     * @throws MQException
     */
    public int receive(int num, E extend) throws MQException;
}
