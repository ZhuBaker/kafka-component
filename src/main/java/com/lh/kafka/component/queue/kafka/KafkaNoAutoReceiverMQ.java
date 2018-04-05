package com.lh.kafka.component.queue.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.lh.kafka.component.queue.kafka.support.Commit;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.core.io.Resource;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.adapter.KafkaMessageAdapter;
import com.lh.kafka.component.queue.kafka.client.consumer.IKafkaMsReceiverClient;
import com.lh.kafka.component.queue.kafka.cons.KafkaConstants;
import com.lh.kafka.component.queue.kafka.thread.KafkaThreadFactory;
import com.lh.kafka.component.queue.kafka.thread.MsHandlerNoAutoThread;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午4:54:36
 * 说明：此方式未默认非自动消费方式（可以根据业务手动消费）
 */
public class KafkaNoAutoReceiverMQ<K, V> extends KafakaMQ<K, V> implements IKafkaNoAutoReceiverMQ<K, V> {

    /**
     * 提交方式：默认同步提交（不可以设置自动提交）
     */
    private Commit commit = Commit.SYNC_COMMIT;
    
    /**
     * 接收本地线程
     */
    private BlockingQueue<ConsumerRecord<K, V>> blockingQueue;
    
    /**
     * 消息接收处理线程
     */
    private List<MsHandlerNoAutoThread<K,V>> msHandlerThreads = new ArrayList<MsHandlerNoAutoThread<K,V>>();

    /**
     * 构造方法
     * @param config
     * @param messageAdapter
     */
    public KafkaNoAutoReceiverMQ(Resource config, KafkaMessageAdapter<K, V> messageAdapter) {
        super(config, messageAdapter);
        
        //设置不自动提交
        if(getProps() != null){
            getProps().setProperty(KafkaConstants.ENABLE_AUTO_COMMIT, "false");
        }
    }

    /**
     * 构造方法
     * @param config
     * @param messageAdapter
     * @param msPollTimeout
     */
    public KafkaNoAutoReceiverMQ(Resource config, KafkaMessageAdapter<K, V> messageAdapter, long msPollTimeout) {
        super(config, messageAdapter, msPollTimeout);

        //设置不自动提交
        if(getProps() != null){
            getProps().setProperty(KafkaConstants.ENABLE_AUTO_COMMIT, "false");
        }
    }

    public Commit getCommit() {
        return commit;
    }

    public void setCommit(Commit commit) {
        this.commit = commit;

        //判断是否不用自动提交
        if (this.commit.equals(Commit.AUTO_COMMIT)){
            throw new IllegalArgumentException("Please set method: KafkaReceiverMQ" +
                    ".setCommit(Commit.ASYNC_COMMIT|Commit.SYNC_COMMIT)");
        }
    }
    
    public BlockingQueue<ConsumerRecord<K, V>> getBlockingQueue() {
        return blockingQueue;
    }

    public void setBlockingQueue(BlockingQueue<ConsumerRecord<K, V>> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void destroy() {
        //阻塞等待队列释放
        if (blockingQueue != null){
            while (!blockingQueue.isEmpty()){
                logger.info("Waitting local queue empty.Current size : " + blockingQueue.size());
            }
        }

        //关闭所有的处理线程
        for(MsHandlerNoAutoThread<K, V> msHandlerThread : msHandlerThreads){
            msHandlerThread.shutdown();
        }

        //关闭消息处理线程池
        if(handlerExecutorService != null){
            handlerExecutorService.shutdown();
            //此处保证再次调用receive时候，重新初始化
            handlerExecutorService = null;
            logger.info("Message handler thread pool closed.");
        }

        //标记不在执行
        running.set(false);
    }
    
    @Override
    public int receive(int num) throws MQException {
        //判断是否是手动提交模式
        String commit = getProps().getProperty(KafkaConstants.ENABLE_AUTO_COMMIT);
        if(commit == null || "true".equals(commit)){
            //自动提交模式下
            throw new MQException("Please set config enable.auto.commit=false.Set method: KafkaReceiverMQ."
                    + "setCommit(Commit.ASYNC_COMMIT|Commit.SYNC_COMMIT)");
        }

        // 使用模式：MODEL_2
        if(blockingQueue == null){
            blockingQueue = new LinkedBlockingQueue<ConsumerRecord<K, V>>(this.asyncQueueSize);
        }

        IKafkaMsReceiverClient<K, V> receiver = null;
        String topic = messageAdapter.getKafkaTopic().getTopic();
        if(handlerExecutorService == null){
            //获取一个新的接收器
            receiver = getNewReceiver();
            int partitionCount = receiver.getPartitionCount(topic);
            if(this.poolSize == 0 || this.poolSize > partitionCount){
                setPoolSize(partitionCount);
            }
            
            //初始化接收线程池(线程池大小由partition决定)
            handlerExecutorService = Executors.newFixedThreadPool(partitionCount, new KafkaThreadFactory(topic));
            
            //初始化处理线程
            int handleSize = this.getPoolSize() * this.getAsyncHandleCoefficient() + 1;
            for (int i = 0; i < handleSize; i++) {
                Properties properties = (Properties) props.clone();
                properties.setProperty(KafkaConstants.CLIENT_ID, getClientId() + "-" + topic + "-" + i);
                
                MsHandlerNoAutoThread<K, V> msHandlerThread = new MsHandlerNoAutoThread<K, V>(messageAdapter, blockingQueue);
                msHandlerThreads.add(msHandlerThread);
                handlerExecutorService.submit(msHandlerThread);
            }
            logger.info("Message receiver mq handle thread initialized size:{}.", handleSize);
        }
        
        Properties properties = (Properties) props.clone();
        properties.setProperty(KafkaConstants.CLIENT_ID, getClientId() + "-" + topic);
        
        if(receiver == null){
            //获取一个新的接收器
            receiver = getNewReceiver();
        }
        
        return receiveHandle(num,receiver, topic);
    }

    /**
     * 处理
     * @param num
     * @param receiver
     * @param topic
     * @return
     */
    protected int receiveHandle(int num, IKafkaMsReceiverClient<K, V> receiver, String topic) {
        KafkaTopic kafkaTopic = new KafkaTopic(topic);
        logger.info("Message receiver start. subscribe topic:" + kafkaTopic.getTopic());

        int count = 0;
        //订阅主题
        receiver.subscribe(Arrays.asList(kafkaTopic.getTopic()));
        try {
            //阻塞接收
            while (running.get() || count == (num - 1)) {
                ConsumerRecords<K, V> records = receiver.poll(this.msPollTimeout);
                if(records.isEmpty()){
                    logger.info("Message has empty.topic:" + kafkaTopic.getTopic());
                    return count;
                }

                switch (this.model) {
                    case MODEL_1:
                        count = doModel1Run(receiver, records, count, num);
                        break;
                    case MODEL_2:
                        count = doModel2Run(receiver, records, count, num);
                        break;
                    default:
                        logger.warn("Receive message [no-auto] no handle.Because property [model] is null.");
                        break;
                }

                if(this.msReceiverThreadSleepTime > 0){
                    try {
                        Thread.currentThread().sleep(this.msReceiverThreadSleepTime);
                    } catch (InterruptedException e) {
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            //归还接收器
            returnReceiver(receiver);
        }

        logger.info("Message receiver end. subscribe topic:" + kafkaTopic.getTopic());
        return count;
    }

    /**
     * Model1方式消息接收处理（直接处理）
     * @param receiver
     * @param records
     * @param count
     * @param num
     * @return
     */
    private int doModel1Run(IKafkaMsReceiverClient<K, V> receiver, ConsumerRecords<K, V> records
            , int count, int num) {
        for (ConsumerRecord<K, V> record : records) {
            try {
                messageAdapter.adapter(record);
            } catch (MQException e) {
                logger.error("Receive message [no-auto] failed.topic: " + record.topic()
                        + ",offset: " + record.offset()
                        + ",partition: " + record.partition(), e);
            } finally {
                receiver.commit(record, this.commit);
                count = count + 1;
                if(count == num){
                    break;
                }
            }
        }
        return count;
    }

    /**
     * Model2方式消息接收处理（放入队列）
     * @param receiver
     * @param records
     * @param count
     * @param num
     * @return
     */
    private int doModel2Run(IKafkaMsReceiverClient<K, V> receiver, ConsumerRecords<K, V> records
            , int count, int num) {
        for (ConsumerRecord<K, V> record : records) {
            try {
                //阻塞方式入队
                this.blockingQueue.put(record);
            } catch (InterruptedException e) {
                logger.error("Receive message queue put failed.", e);
            } finally {
                receiver.commit(record, this.commit);
                count = count + 1;
                if(count == num){
                    break;
                }
            }
        }
        return count;
    }
}
