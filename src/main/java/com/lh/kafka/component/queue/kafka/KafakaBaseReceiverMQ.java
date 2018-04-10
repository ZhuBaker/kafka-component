package com.lh.kafka.component.queue.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lh.kafka.component.queue.kafka.support.Model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import com.lh.kafka.component.queue.kafka.client.consumer.IKafkaMsReceiverClient;
import com.lh.kafka.component.queue.kafka.client.consumer.KafkaMsReceiverClient;
import com.lh.kafka.component.queue.kafka.cons.KafkaConstants;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月4日 下午4:53:15
 * 说明：抽象消息队列
 */
public abstract class KafakaBaseReceiverMQ implements IKafakaMQ {
    
    protected static final Logger logger = LoggerFactory.getLogger(KafakaBaseReceiverMQ.class);
    
    /**
     * 是否运行：默认不运行
     */
    protected AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 配置属性
     */
    protected Properties props = new Properties();
    
    /**   
     * 异步处理消息队列长度(Model是Model_2时生效 )
     */
    protected int asyncQueueSize = 100000;
    
    /**
     * 线程池的大小：（默认启动一个消费者，如过需要启动多个请设置消费者个数 ）
     */
    protected int poolSize = 1;

    /**
     * 消息拉取超时时间：kafka poll timeout time(ms)
     */
    protected long msPollTimeout = Long.MAX_VALUE;

    /**
     * 消息接收线程睡眠时间:防止cpu使用率过高
     */
    protected long msReceiverThreadSleepTime = 0;
    
    /**
     * 消息异步处理线程个数系数(Model是Model_2时生效 )
     * <p style="color:red;">
     * (When use MODEL_2, the handle thread pool size is (poolSize * handleMultiple + 1))
     * </p>
     */
    protected int asyncHandleCoefficient = 2;

    /**
     * 接收模式：默认使用模式1（数据接收与业务处理在同一线程中（并发取决于队列分区））
     */
    protected Model model = Model.MODEL_1;
    
    /**
     * 资源
     */
    private Resource resource;
    
    /**
     * 消息接收线程：在同一业务（本线程）中执行
     */
    protected ExecutorService receiverExecutorService;
    
    /**   
     * 消息处理线程池
     */
    protected ExecutorService handlerExecutorService;
    
    /**
     * 构造方法
     * @param config
     */
    public KafakaBaseReceiverMQ(Resource config) {
        setConfig(config);
    }

    /**
     * 构造方法
     * @param config
     * @param msPollTimeout
     */
    public KafakaBaseReceiverMQ(Resource config, long msPollTimeout) {
        setConfig(config);
        setMsPollTimeout(msPollTimeout);
    }

    @Override
    public synchronized boolean isRunning() {
        return running.get();
    }

    @Override
    public synchronized void shutdown() {
        this.running.set(false);
    }

    public IKafkaMsReceiverClient<byte[], byte[]> getNewReceiverClient() {
        return new KafkaMsReceiverClient<byte[], byte[]>((Properties) props.clone());
    }
    
    public void returnReceiverClient(IKafkaMsReceiverClient<byte[], byte[]> receiverClient) {
        if(receiverClient != null){
            receiverClient.shutDown();
        }
    }
    
    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public int getAsyncQueueSize() {
        return asyncQueueSize;
    }

    public void setAsyncQueueSize(int asyncQueueSize) {
        this.asyncQueueSize = asyncQueueSize;
    }
    
    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public long getMsPollTimeout() {
        return msPollTimeout;
    }

    public void setMsPollTimeout(long msPollTimeout) {
        this.msPollTimeout = msPollTimeout;
    }

    public long getMsReceiverThreadSleepTime() {
        return msReceiverThreadSleepTime;
    }

    public void setMsReceiverThreadSleepTime(long msReceiverThreadSleepTime) {
        this.msReceiverThreadSleepTime = msReceiverThreadSleepTime;
    }
    
    public int getAsyncHandleCoefficient() {
        return asyncHandleCoefficient;
    }

    public void setAsyncHandleCoefficient(int asyncHandleCoefficient) {
        this.asyncHandleCoefficient = asyncHandleCoefficient;
    }

    public Model getModel() {
        return model;
    }

    public void setModel(Model model) {
        this.model = model;
    }
    
    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }
    
    /**
     * Sets resource.
     *
     * @param resource 
     */
    protected void setConfig(Resource resource) {
        this.resource = resource;
        try {
            PropertiesLoaderUtils.fillProperties(this.props, this.resource);
            Resource kafkaServersConfig = new DefaultResourceLoader().getResource("kafka/kafka-servers.properties");
            PropertiesLoaderUtils.fillProperties(props, kafkaServersConfig);
        } catch (IOException e) {
            logger.error("Fill properties failed.", e);
        }
    }
    
    /**
     * 取得客户端id
     * @return
     */
    public String getClientId() {
        return this.props.getProperty(KafkaConstants.CLIENT_ID, "client-new-consumer");
    }

    /**
     * 获取消费组id
     * @return
     */
    public String getGroupId() {
        return this.props.getProperty(KafkaConstants.GROUP_ID, "group-new-consumer");
    }
}
