package com.lh.kafka.component.queue.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
                logger.info("Waitting local queue empty.");
            }
        }
        
        //TODO：
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
        if(receiverExecutorService == null){
            //获取一个新的接收器
            receiver = getNewReceiver();
            int partitionCount = receiver.getPartitionCount(topic);
            if(this.poolSize == 0 || this.poolSize > partitionCount){
                setPoolSize(partitionCount);
            }
            
            //初始化接收线程池(线程池大小由partition决定)
            receiverExecutorService = Executors.newFixedThreadPool(partitionCount, new KafkaThreadFactory(topic));
            
            //初始化处理线程
            int handleSize = this.getPoolSize() * this.getAsyncHandleCoefficient() + 1;
            for (int i = 0; i < handleSize; i++) {
                Properties properties = (Properties) props.clone();
                properties.setProperty(KafkaConstants.CLIENT_ID, getClientId() + "-" + topic + "-" + i);
                
                MsHandlerNoAutoThread<K, V> msHandlerThread = new MsHandlerNoAutoThread<K, V>(messageAdapter, blockingQueue);
                msHandlerThreads.add(msHandlerThread);
                receiverExecutorService.submit(msHandlerThread);
            }
            logger.info("Message receiver mq handle thread initialized size:{}.", handleSize);
        }
        
        Properties properties = (Properties) props.clone();
        properties.setProperty(KafkaConstants.CLIENT_ID, getClientId() + "-" + topic);
        
        if(receiver == null){
            //获取一个新的接收器
            receiver = getNewReceiver();
        }
        
        return receiveHandle(receiver);
    }

    /**
     * 处理
     * @param receiver
     * @return
     */
    protected int receiveHandle(IKafkaMsReceiverClient<K, V> receiver) {
        try {
            
            
            
        } catch (Exception e) {
            // TODO: handle exception
        } finally {
            returnReceiver(receiver);
        }
        
        // TODO Auto-generated method stub
        return 0;
    }
    
}
