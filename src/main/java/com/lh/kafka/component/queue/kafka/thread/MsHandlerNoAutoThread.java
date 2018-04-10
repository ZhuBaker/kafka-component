package com.lh.kafka.component.queue.kafka.thread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lh.kafka.component.queue.exception.MQException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lh.kafka.component.queue.kafka.adapter.KafkaNoAutoMessageAdapter;
import com.lh.kafka.component.queue.kafka.support.NoAutoConsumerRecordQueueItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月4日 下午6:15:39
 * 说明：消息处理线程（从队列里拿出数据）
 */
public class MsHandlerNoAutoThread<K, V, E> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MsHandlerNoAutoThread.class);

    /**
     * 是否关闭
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /**
     * 消息适配器
     */
    private KafkaNoAutoMessageAdapter<?, ?, E> noAutoMessageAdapter;

    /**
     * 本地消费队列
     */
    private BlockingQueue<NoAutoConsumerRecordQueueItem<K, V, E>> blockingQueue;

    /**
     * 消息处理线程睡眠时间:防止cpu使用率过高
     */
    private long msHandlerThreadSleepTime = 0;

    /**
     * 构造方法
     * @param noAutoMessageAdapter
     * @param blockingQueue
     */
    public MsHandlerNoAutoThread(KafkaNoAutoMessageAdapter<?, ?, E> noAutoMessageAdapter,
            BlockingQueue<NoAutoConsumerRecordQueueItem<K, V, E>> blockingQueue) {
        this.noAutoMessageAdapter = noAutoMessageAdapter;
        this.blockingQueue = blockingQueue;
    }


    /**
     * 构造方法
     * @param noAutoMessageAdapter
     * @param blockingQueue
     * @param msHandlerThreadSleepTime
     */
    public MsHandlerNoAutoThread(KafkaNoAutoMessageAdapter<?, ?, E> noAutoMessageAdapter
            ,BlockingQueue<NoAutoConsumerRecordQueueItem<K, V, E>> blockingQueue, long msHandlerThreadSleepTime) {
        this.noAutoMessageAdapter = noAutoMessageAdapter;
        this.blockingQueue = blockingQueue;
        this.msHandlerThreadSleepTime = msHandlerThreadSleepTime;
    }

    @SuppressWarnings("static-access")
    @Override
    public void run() {
        logger.info("Message handler thread [" + Thread.currentThread().getName() + "] start success.");

        while (!closed.get()) {
            NoAutoConsumerRecordQueueItem<K, V, E> item = null;
            try {
                item = blockingQueue.take();
            } catch (InterruptedException e) {
                logger.error("BlockingQueue take failed.", e);
            }

            if(item != null){
                try {
                    noAutoMessageAdapter.adapter(item.getConsumerRecord(), item.getExtend());
                } catch (MQException e) {
                    ConsumerRecord<K, V> record = item.getConsumerRecord();
                    if(record != null){
                        logger.error("Receive message failed.topic: " + record.topic()
                                + ",offset: " + record.offset()
                                + ",partition: " + record.partition(), e);
                    }else {
                        logger.error("Receive message failed.record is null.", e);
                    }
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
     * 关闭运行线程
     */
    public void shutdown() {
        closed.set(true);
    }
}
