package com.lh.kafka.component.queue.kafka.thread;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.adapter.KafkaMessageAdapter;
import com.lh.kafka.component.queue.kafka.client.consumer.IKafkaMsReceiverClient;
import com.lh.kafka.component.queue.kafka.support.Batch;
import com.lh.kafka.component.queue.kafka.support.Commit;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;
import com.lh.kafka.component.queue.kafka.support.Model;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午1:59:02
 * 说明：消息接收线程
 */
public class MsReceiverThread<K, V> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MsReceiverThread.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private IKafkaMsReceiverClient<K, V> receiver;
    private KafkaMessageAdapter<K, V> messageAdapter;
    private BlockingQueue<ConsumerRecords<K, V>> blockingQueue;
    private Model model = Model.MODEL_1;
    private Batch batch = Batch.NO;
    private Commit commit = Commit.AUTO_COMMIT;
    private long msReceiverThreadSleepTime = 0;
    private long msPollTimeout = 0;
    private KafkaTopic kafkaTopic;

    /**
     * 构造方法
     * @param receiver  客户端消息接收对象
     * @param messageAdapter    消息适配器
     * @param blockingQueue 本地消息接收队列
     * @param model  接收模式
     * @param batch  是否批量消费
     * @param commit    提交方式
     * @param msReceiverThreadSleepTime 接收消息睡眠时间
     * @param msPollTimeout 消息拉取超时时间
     * @param kafkaTopic    消息消费的topic
     */
    public MsReceiverThread(IKafkaMsReceiverClient<K, V> receiver,
            KafkaMessageAdapter<K, V> messageAdapter,
            BlockingQueue<ConsumerRecords<K, V>> blockingQueue, Model model,
            Batch batch, Commit commit, long msReceiverThreadSleepTime,
            long msPollTimeout, KafkaTopic kafkaTopic) {
        this.receiver = receiver;
        this.messageAdapter = messageAdapter;
        this.blockingQueue = blockingQueue;
        this.model = model;
        this.batch = batch;
        this.commit = commit;
        this.msReceiverThreadSleepTime = msReceiverThreadSleepTime;
        this.msPollTimeout = msPollTimeout;
        this.kafkaTopic = kafkaTopic;
    }
    
    public IKafkaMsReceiverClient<K, V> getReceiver() {
        return receiver;
    }

    public void setReceiver(IKafkaMsReceiverClient<K, V> receiver) {
        this.receiver = receiver;
    }

    public KafkaMessageAdapter<K, V> getMessageAdapter() {
        return messageAdapter;
    }

    public void setMessageAdapter(KafkaMessageAdapter<K, V> messageAdapter) {
        this.messageAdapter = messageAdapter;
    }

    public BlockingQueue<ConsumerRecords<K, V>> getBlockingQueue() {
        return blockingQueue;
    }

    public void setBlockingQueue(BlockingQueue<ConsumerRecords<K, V>> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    public Model getModel() {
        return model;
    }

    public void setModel(Model model) {
        this.model = model;
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Commit getCommit() {
        return commit;
    }
    
    public void setCommit(Commit commit) {
        this.commit = commit;
    }

    public long getMsReceiverThreadSleepTime() {
        return msReceiverThreadSleepTime;
    }

    public void setMsReceiverThreadSleepTime(long msReceiverThreadSleepTime) {
        this.msReceiverThreadSleepTime = msReceiverThreadSleepTime;
    }

    public long getMsPollTimeout() {
        return msPollTimeout;
    }

    public void setMsPollTimeout(long msPollTimeout) {
        this.msPollTimeout = msPollTimeout;
    }

    public KafkaTopic getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(KafkaTopic kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    @SuppressWarnings("static-access")
    @Override
    public void run() {
        logger.info("Message receiver thread ["+ Thread.currentThread().getName() 
                + "] start success. subscribe topic:" + kafkaTopic.getTopic());
        
        //订阅主题
        receiver.subscribe(Arrays.asList(kafkaTopic.getTopic()));
        
        try {
            while(!closed.get()){
                ConsumerRecords<K, V> records = receiver.poll(this.msPollTimeout);
                switch (this.model) {
                case MODEL_1:
                    doModel1Run(records);
                    break;
                case MODEL_2:
                    doModel2Run(records);
                    break;
                default:
                    logger.warn("Receive message no handle.Because property [model] is null.");
                    break;
                }
                
                if(this.msReceiverThreadSleepTime > 0){
                    try {
                        Thread.currentThread().sleep(this.msReceiverThreadSleepTime);
                    } catch (InterruptedException e) {
                    }
                }
            }
        } catch (WakeupException e) {
            if(!closed.get()){
                throw new WakeupException();
            }
        }finally {
            receiver.close();
        }
        
        logger.info("Message receiver thread ["+ Thread.currentThread().getName() + "] end success.");
    }
    
    /**
     * Model1方式消息接收处理（直接处理）
     * @param records
     */
    private void doModel1Run(ConsumerRecords<K, V> records) {
        switch (this.batch) {
        case YES:
            try {
                messageAdapter.adapter(records);
            } catch (MQException e) {
                logger.error("Receive message failed. fail size: " + records.count(), e);
            } finally {
                receiver.batchCommit(this.commit);
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
                } finally {
                    receiver.commit(record, this.commit);
                }
            }
            break;
        default:
            logger.warn("Receive message no handle.Because property [batch] is null.");
            break;
        }
    }

    /**
     * Model2方式消息接收处理（放入队列）
     * @param records
     */
    private void doModel2Run(ConsumerRecords<K, V> records) {
        try {
            //阻塞方式入队
            this.blockingQueue.put(records);
        } catch (InterruptedException e) {
            logger.error("Receive message queue put failed.", e);
        }
        
        //提交偏移量（MsHandlerThread中不需要提交偏移量）
        receiver.batchCommit(this.commit);
    }

    /**
     * 关闭线程
     */
    public void shutdown() {
        this.closed.set(true);
        if(this.receiver != null){
            this.receiver.wakeup();
        }
    }
}
