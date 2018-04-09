package com.lh.kafka.component.queue.kafka.thread;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lh.kafka.component.queue.exception.MQException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lh.kafka.component.queue.kafka.adapter.KafkaMessageAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月4日 下午6:15:39
 * 说明：消息处理线程（从队列里拿出数据）
 */
public class MsHandlerNoAutoThread<K, V> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MsHandlerNoAutoThread.class);

    /**
     * 是否关闭
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 消息适配器
     */
    private KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter;

    /**
     * 本地消费队列
     */
    private BlockingQueue<ConsumerRecord<K, V>> blockingQueue;

    /**
     * 消息处理线程睡眠时间:防止cpu使用率过高
     */
    private long msHandlerThreadSleepTime = 0;

    /**
     * 构造方法
     * @param messageAdapter
     * @param blockingQueue
     */
    public MsHandlerNoAutoThread(KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter,
            BlockingQueue<ConsumerRecord<K, V>> blockingQueue) {
        this.messageAdapter = messageAdapter;
        this.blockingQueue = blockingQueue;
    }


    /**
     * 构造方法
     * @param messageAdapter
     * @param blockingQueue
     * @param msHandlerThreadSleepTime
     */
    public MsHandlerNoAutoThread(KafkaMessageAdapter<? extends Serializable, ? extends Serializable> messageAdapter,
                                 BlockingQueue<ConsumerRecord<K, V>> blockingQueue,
                                 long msHandlerThreadSleepTime) {
        this.messageAdapter = messageAdapter;
        this.blockingQueue = blockingQueue;
        this.msHandlerThreadSleepTime = msHandlerThreadSleepTime;
    }

    @SuppressWarnings("static-access")
    @Override
    public void run() {
        logger.info("Message handler thread [" + Thread.currentThread().getName() + "] start success.");

        while (!closed.get()) {
            ConsumerRecord<K, V> record = null;
            try {
                record = blockingQueue.take();
            } catch (InterruptedException e) {
                logger.error("BlockingQueue take failed.", e);
            }

            if(record != null){
                try {
                    messageAdapter.adapter(record);
                } catch (MQException e) {
                    logger.error("Receive message failed.topic: " + record.topic()
                            + ",offset: " + record.offset()
                            + ",partition: " + record.partition(), e);
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
