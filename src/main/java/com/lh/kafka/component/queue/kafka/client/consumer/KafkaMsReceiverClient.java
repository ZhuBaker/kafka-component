package com.lh.kafka.component.queue.kafka.client.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lh.kafka.component.queue.kafka.cons.KafkaConstants;
import com.lh.kafka.component.queue.kafka.support.Commit;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午11:52:15
 * 说明：kafka消息接收客户端客户端
 */
public class KafkaMsReceiverClient<K, V> extends KafkaConsumer<K, V> implements IKafkaMsReceiverClient<K, V> {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaMsReceiverClient.class);
    
    /**
     * 构造方法
     * @param configs
     */
    public KafkaMsReceiverClient(Map<String, Object> configs) {
        super(configs);
    }

    /**
     * 构造方法
     * @param properties
     */
    public KafkaMsReceiverClient(Properties properties) {
        super(properties);
    }

    /**
     * 构造方法
     * @param configs
     * @param keyDeserializer
     * @param valueDeserializer
     */
    public KafkaMsReceiverClient(Map<String, Object> configs,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
    }

    /**
     * 构造方法
     * @param properties
     * @param keyDeserializer
     * @param valueDeserializer
     */
    public KafkaMsReceiverClient(Properties properties,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(properties, keyDeserializer, valueDeserializer);
    }

    @Override
    public synchronized List<V> receive(String topic, int partition, long beginOffset,
            long readOffset) {
        if(readOffset <= 0){
            throw new IllegalArgumentException("Read offset param(readOffset) value must be a positive");
        }
        
        long earliestOffset = getEarliestOffset(topic, partition);
        if(beginOffset <= earliestOffset){
            //当前最早的偏移量大于指定开始偏移量，已当前最早偏移量为准
            beginOffset = earliestOffset;
        }
        
        long latestOffset = getLatestOffset(topic, partition);
        if((beginOffset + readOffset) > latestOffset){
            //不够读取，读取偏移量=最后偏移量-最早的偏移量
            readOffset = latestOffset - beginOffset;
        }
        
        this.assign(Arrays.asList(new TopicPartition(topic, partition)));
        this.seek(new TopicPartition(topic, partition), beginOffset);
        
        //最后读取的偏移量
        long endOffset = beginOffset + readOffset -1;
        List<V> list = new ArrayList<V>();
        
        boolean flag = true;
        while (flag) {
             ConsumerRecords<K, V> records = this.poll(KafkaConstants.POLL_TIME_OUT);
             for (ConsumerRecord<K, V> record : records) {
                 long currOffset = record.offset();
                 if((currOffset == latestOffset -1) || (currOffset > endOffset)){
                     //读取完毕，退出循环
                     flag = false;
                     break;
                 }
                 list.add(record.value());
            }
        }
        
        return list;
    }

    @Override
    public synchronized Map<K, V> receiveMap(String topic, int partition,
            long beginOffset, long readOffset) {
        if(readOffset <= 0){
            throw new IllegalArgumentException("Read offset param(readOffset) value must be a positive");
        }
        
        long earliestOffset = getEarliestOffset(topic, partition);
        if(beginOffset <= earliestOffset){
            //当前最早的偏移量大于指定开始偏移量，已当前最早偏移量为准
            beginOffset = earliestOffset;
        }
        
        long latestOffset = getLatestOffset(topic, partition);
        if((beginOffset + readOffset) > latestOffset){
            //不够读取，读取偏移量=最后偏移量-最早的偏移量
            readOffset = latestOffset - beginOffset;
        }
        
        this.assign(Arrays.asList(new TopicPartition(topic, partition)));
        this.seek(new TopicPartition(topic, partition), beginOffset);
        
        //最后读取的偏移量
        long endOffset = beginOffset + readOffset -1;
        Map<K, V> map = new HashMap<K, V>();
        
        boolean flag = true;
        while (flag) {
             ConsumerRecords<K, V> records = this.poll(KafkaConstants.POLL_TIME_OUT);
             for (ConsumerRecord<K, V> record : records) {
                 long currOffset = record.offset();
                 if((currOffset == latestOffset -1) || (currOffset > endOffset)){
                     //读取完毕，退出循环
                     flag = false;
                     break;
                 }
                 map.put(record.key(), record.value());
            }
        }
        
        return map;
    }

    @Override
    public int getPartitionCount(String topic) {
        List<PartitionInfo> list = this.partitionsFor(topic);
        return list != null ? list.size() : 0;
    }

    @Override
    public long getEarliestOffset(String topic, int partition) {
        this.assign(Arrays.asList(new TopicPartition(topic, partition)));
        this.seekToBeginning(Arrays.asList(new TopicPartition(topic, partition)));
        return this.position(new TopicPartition(topic, partition));
    }

    @Override
    public long getLatestOffset(String topic, int partition) {
        this.assign(Arrays.asList(new TopicPartition(topic, partition)));
        this.seekToEnd(Arrays.asList(new TopicPartition(topic, partition)));
        return this.position(new TopicPartition(topic, partition));
    }

    @Override
    public void commit(ConsumerRecord<K, V> record, Commit commit) {
        switch (commit) {
        case SYNC_COMMIT:   //同步提交
            this.commitSync(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), 
                    new OffsetAndMetadata(record.offset() + 1)));
            break;
        case ASYNC_COMMIT:  //异步提交
            this.commitAsync(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), 
                    new OffsetAndMetadata(record.offset() + 1)), new OffsetCommitCallback() {
                        
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                Exception exception) {
                            //异步提交执行完后的回掉
                            if(exception != null){
                                logger.error("Offset commit failed.", exception);
                            }
                        }
                    });
            break;
        default:
            break;
        }
    }

    @Override
    public void batchCommit(Commit commit) {
        switch (commit) {
        case SYNC_COMMIT:   //同步提交
            this.commitSync();
            break;
        case ASYNC_COMMIT:  //异步提交
            this.commitAsync();
            break;
        default:
            break;
        }
    }

    @Override
    public void shutDown() {
        synchronized (this) {
            this.close();
        }
    }
}
