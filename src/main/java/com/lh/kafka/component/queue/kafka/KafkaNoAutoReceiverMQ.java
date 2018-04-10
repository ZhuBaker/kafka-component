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
import com.lh.kafka.component.queue.kafka.support.Model;
import com.lh.kafka.component.queue.kafka.support.NoAutoConsumerRecordQueueItem;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.core.io.Resource;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.adapter.KafkaNoAutoMessageAdapter;
import com.lh.kafka.component.queue.kafka.client.consumer.IKafkaMsReceiverClient;
import com.lh.kafka.component.queue.kafka.cons.KafkaConstants;
import com.lh.kafka.component.queue.kafka.thread.KafkaThreadFactory;
import com.lh.kafka.component.queue.kafka.thread.MsHandlerNoAutoThread;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午4:54:36
 * 说明：此方式未默认非自动消费方式（可以根据业务手动消费）
 * <p>
 *      说明：默认使用model1模式消费，调用receive接口时是同步方式；若采用model2方式消费receive接口返回的值会与hanlde存在异步关系
 * </p>
 */
public class KafkaNoAutoReceiverMQ<E> extends KafakaBaseReceiverMQ implements IKafkaNoAutoReceiverMQ<E> {

    /**
     * 提交方式：默认同步提交（不可以设置自动提交）
     */
    private Commit commit = Commit.SYNC_COMMIT;
    
    /**
     * 接收本地线程
     */
    private BlockingQueue<NoAutoConsumerRecordQueueItem<byte[],byte[], E>> blockingQueue;
    
    /**
     * 消息适配器
     */
    private KafkaNoAutoMessageAdapter<?, ?, E> noAutoMessageAdapter;
    
    /**
     * 消息接收处理线程
     */
    private List<MsHandlerNoAutoThread<byte[],byte[], E>> msHandlerThreads = new ArrayList<MsHandlerNoAutoThread<byte[],byte[], E>>();

    /**
     * 构造方法
     * @param config
     * @param messageAdapter
     */
    public KafkaNoAutoReceiverMQ(Resource config, KafkaNoAutoMessageAdapter<?, ?, E> noAutoMessageAdapter) {
        super(config);
        setNoAutoMessageAdapter(noAutoMessageAdapter);
        
        //初始化配置信息
        initConfig();
    }

    /**
     * 构造方法
     * @param config
     * @param messageAdapter
     * @param msPollTimeout
     */
    public KafkaNoAutoReceiverMQ(Resource config, KafkaNoAutoMessageAdapter<?, ?, E> noAutoMessageAdapter
            , long msPollTimeout) {
        super(config, msPollTimeout);
        setNoAutoMessageAdapter(noAutoMessageAdapter);
        
        //初始化配置信息
        initConfig();
    }
    
    /**
     * 初始化配置信息
     */
    private void initConfig(){
        //设置不自动提交
        if(getProps() != null){
            getProps().setProperty(KafkaConstants.ENABLE_AUTO_COMMIT, "false");
        }
        
        //设置运行中
        running.set(true);
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
    
    public BlockingQueue<NoAutoConsumerRecordQueueItem<byte[],byte[], E>> getBlockingQueue() {
        return blockingQueue;
    }

    public void setBlockingQueue(BlockingQueue<NoAutoConsumerRecordQueueItem<byte[],byte[], E>> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    public KafkaNoAutoMessageAdapter<?, ?, E> getNoAutoMessageAdapter() {
        return noAutoMessageAdapter;
    }

    public void setNoAutoMessageAdapter(
            KafkaNoAutoMessageAdapter<?, ?, E> noAutoMessageAdapter) {
        this.noAutoMessageAdapter = noAutoMessageAdapter;
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
        for(MsHandlerNoAutoThread<byte[], byte[], E> msHandlerThread : msHandlerThreads){
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
        return receive(num, null);
    }
    
    @Override
    public int receive(int num, E extend) throws MQException {
        //判断是否是手动提交模式
        String commit = getProps().getProperty(KafkaConstants.ENABLE_AUTO_COMMIT);
        if(commit == null || "true".equals(commit)){
            //自动提交模式下
            throw new MQException("Please set config enable.auto.commit=false.Set method: KafkaReceiverMQ."
                    + "setCommit(Commit.ASYNC_COMMIT|Commit.SYNC_COMMIT)");
        }

        //接受kafka消息客户端
        IKafkaMsReceiverClient<byte[], byte[]> receiverClient = null;
        String topic = noAutoMessageAdapter.getKafkaTopic().getTopic();

        // 使用模式：MODEL_2
        if(this.model == Model.MODEL_2){
            if(blockingQueue == null){
                blockingQueue = new LinkedBlockingQueue<NoAutoConsumerRecordQueueItem<byte[],byte[], E>>(this.asyncQueueSize);
            }
            if(handlerExecutorService == null){
                //获取一个新的接收器
                receiverClient = getNewReceiverClient();
                int partitionCount = receiverClient.getPartitionCount(topic);
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
                    
                    MsHandlerNoAutoThread<byte[], byte[], E> msHandlerThread = new MsHandlerNoAutoThread<byte[], byte[]
                            , E>(noAutoMessageAdapter, blockingQueue);
                    msHandlerThreads.add(msHandlerThread);
                    handlerExecutorService.submit(msHandlerThread);
                }
                logger.info("Message receiver mq handle thread initialized size:{}.", handleSize);
            }
        }
        
        Properties properties = (Properties) props.clone();
        properties.setProperty(KafkaConstants.CLIENT_ID, getClientId() + "-" + topic);
        
        if(receiverClient == null){
            //获取一个新的接收器
            receiverClient = getNewReceiverClient();
        }
        
        return receiveHandle(num, receiverClient, topic, extend);
    }

    /**
     * 处理
     * @param num
     * @param receiverClient
     * @param topic
     * @param extend
     * @return
     */
    @SuppressWarnings("static-access")
    protected int receiveHandle(int num, IKafkaMsReceiverClient<byte[], byte[]> receiverClient, String topic, E extend) {
        KafkaTopic kafkaTopic = new KafkaTopic(topic);
        logger.info("Message receiver start. subscribe topic:" + kafkaTopic.getTopic());

        int count = 0;
        //订阅主题
        receiverClient.subscribe(Arrays.asList(kafkaTopic.getTopic()));
        try {
            //阻塞接收
            while (running.get() || count <= (num - 1)) {
                ConsumerRecords<byte[], byte[]> records = receiverClient.poll(this.msPollTimeout);
                if(records.isEmpty()){
                    logger.info("Message has empty.topic:" + kafkaTopic.getTopic());
                    return count;
                }

                switch (this.model) {
                    case MODEL_1:
                        count = doModel1Run(receiverClient, records, extend, count, num);
                        break;
                    case MODEL_2:
                        count = doModel2Run(receiverClient, records, extend, count, num);
                        break;
                    default:
                        logger.warn("Receive message [no-auto] no handle.Because property [model] is null.");
                        break;
                }
                
                if(count == num){
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
            returnReceiverClient(receiverClient);
        }

        logger.info("Message receiver end. subscribe topic:" + kafkaTopic.getTopic());
        return count;
    }

    /**
     * Model1方式消息接收处理（直接处理）
     * @param receiver
     * @param records
     * @param extend
     * @param count
     * @param num
     * @return
     */
    private int doModel1Run(IKafkaMsReceiverClient<byte[], byte[]> receiver, ConsumerRecords<byte[], byte[]> records
            , E extend, int count, int num) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            try {
                noAutoMessageAdapter.adapter(record, extend);
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
     * @param extend
     * @param count
     * @param num
     * @return
     */
    private int doModel2Run(IKafkaMsReceiverClient<byte[], byte[]> receiver, ConsumerRecords<byte[], byte[]> records
            , E extend, int count, int num) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            try {
                //阻塞方式入队
                this.blockingQueue.put(new NoAutoConsumerRecordQueueItem<byte[], byte[], E>(record, extend));
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
