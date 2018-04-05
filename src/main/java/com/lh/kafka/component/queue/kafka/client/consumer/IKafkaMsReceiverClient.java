package com.lh.kafka.component.queue.kafka.client.consumer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lh.kafka.component.queue.kafka.support.Commit;


/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午11:41:14
 * 说明：kafka消息接收客户端
 */
public interface IKafkaMsReceiverClient<K, V> extends Consumer<K, V>{

    /**
     * 接收
     * @param topic 指定topic主题
     * @param partition  指定partition分区
     * @param beginOffset   其实偏移量
     * @param readOffset    读取的偏移量
     * @return
     */
    public List<V> receive(String topic, int partition, long beginOffset, long readOffset);
    
    /**
     * 通过指定key接收
     * @param topic 指定topic主题
     * @param partition  指定partition分区
     * @param beginOffset   其实偏移量
     * @param readOffset    读取的偏移量
     * @return
     */
    public Map<K, V> receiveMap(String topic, int partition, long beginOffset, long readOffset);
    
    /**
     * 读取partition数量
     * @param topic 指定topic主题
     * @return
     */
    public int getPartitionCount(String topic);

    /**
     * 取得最早的偏移量
     * @param topic 指定topic主题
     * @param partition  指定partition分区
     * @return
     */
    public long getEarliestOffset(String topic, int partition);
    
    /**
     * 取得最新的偏移量
     * @param topic 指定topic主题
     * @param partition  指定partition分区
     * @return
     */
    public long getLatestOffset(String topic, int partition);

    /**
     * 提交
     * @param record
     * @param commit
     */
    public void commit(ConsumerRecord<K, V> record, Commit commit);

    /**
     * 批量提交偏移量
     * @param commit 
     */
    public void batchCommit(Commit commit);

    /**
     * 停止消费
     */
    public void shutDown();
}
