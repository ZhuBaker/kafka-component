package com.lh.kafka.component.queue.kafka.thread;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.adapter.KafkaMessageAdapter;
import com.lh.kafka.component.queue.kafka.support.Batch;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午2:07:17
 * 说明：消息处理线程（从队列里拿出数据）
 */
public class MsHandlerThread<K, V> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MsHandlerThread.class);

    /**
     * 是否关闭
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    /**
     * 是否批量
     */
    private Batch batch = Batch.NO;

    /**
     * 消息适配器
     */
    private KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter;
    
    /**
     * 消费的本地队列
     */
    private BlockingQueue<ConsumerRecords<K, V>> blockingQueue;

    /**
     * 消息处理线程睡眠时间:防止cpu使用率过高
     */
    private long msHandlerThreadSleepTime = 0;

    /**
     * 构造方法
     * @param messageAdapter    消息适配器
     * @param blockingQueue 
     */
    public MsHandlerThread(KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter,
            BlockingQueue<ConsumerRecords<K, V>> blockingQueue) {
        super();
        this.messageAdapter = messageAdapter;
        this.blockingQueue = blockingQueue;
    }

    /**
     * 构造方法
     * @param messageAdapter    消息适配器
     * @param blockingQueue
     * @param msHandlerThreadSleepTime
     */
    public MsHandlerThread(KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter,
                           BlockingQueue<ConsumerRecords<K, V>> blockingQueue,
                           long msHandlerThreadSleepTime) {
        super();
        this.messageAdapter = messageAdapter;
        this.blockingQueue = blockingQueue;
        this.msHandlerThreadSleepTime = msHandlerThreadSleepTime;
    }
    
    /**
     * 构造方法
     * @param batch
     * @param messageAdapter
     * @param blockingQueue
     */
    public MsHandlerThread(Batch batch,
            KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter,
            BlockingQueue<ConsumerRecords<K, V>> blockingQueue) {
        super();
        if(batch != null){
            this.batch = batch;
        }
        this.messageAdapter = messageAdapter;
        this.blockingQueue = blockingQueue;
    }

    /**
     * 构造方法
     * @param batch
     * @param messageAdapter
     * @param blockingQueue
     * @param msHandlerThreadSleepTime
     */
    public MsHandlerThread(Batch batch,
                           KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter,
                           BlockingQueue<ConsumerRecords<K, V>> blockingQueue,
                           long msHandlerThreadSleepTime) {
        super();
        if(batch != null){
            this.batch = batch;
        }
        this.messageAdapter = messageAdapter;
        this.blockingQueue = blockingQueue;
        this.msHandlerThreadSleepTime = msHandlerThreadSleepTime;
    }

    @SuppressWarnings("static-access")
    @Override
    public void run() {
        logger.info("Message handler thread [" + Thread.currentThread().getName() + "] start success.");
        while (!closed.get()) {
            ConsumerRecords<K, V> records = null;
            try {
                records = blockingQueue.take();
            } catch (InterruptedException e) {
                logger.error("BlockingQueue take failed.", e);
            }
            
            if(records != null){
                switch (this.batch) {
                case YES:
                    try {
                        messageAdapter.adapter(records);
                    } catch (MQException e) {
                        logger.error("Receive message failed. fail size: " + records.count(), e);
                    }
                    break;
                case NO:
                    for (ConsumerRecord<K, V> record : records) {
                        try {
                            messageAdapter.adapter(record);
                        } catch (MQException e) {
                            logger.error("Receive message failed.topic: " + record.topic()
                                    + ",offset: " + record.offset() 
                                    + ",partition: " + record.partition(), e);
                        }
                    }
                    break;
                default:
                    logger.warn("Receive message no handle.Because property [batch] is null.");
                    break;
                }
            }

            if(this.msHandlerThreadSleepTime > 0){
                try {
                    Thread.currentThread().sleep(this.msHandlerThreadSleepTime);
                } catch (InterruptedException e) {
                }
            }
        }

        logger.info("Message handler thread ["+ Thread.currentThread().getName() + "] end success.");
    }

    /**
     * 关闭数据
     */
    public void shutdown() {
        closed.set(true);
    }
}
