package com.lh.kafka.component.queue.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.client.producer.IKafkaMsSenderClient;
import com.lh.kafka.component.queue.kafka.client.producer.KafkaMsSenderClient;
import com.lh.kafka.component.queue.kafka.codec.KafkaMessageEncoder;
import com.lh.kafka.component.queue.kafka.exception.KafkaUnrecoverableException;
import com.lh.kafka.component.queue.kafka.support.KafkaTopic;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午11:04:17
 * 说明：
 */
public class KafkaSenderMQ<K, V> implements IKafkaSenderMQ<K, V> {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaSenderMQ.class);
    
    /**
     * 配置属性
     */
    protected Properties props = new Properties();
    
    /**
     * kafka编码器
     */
    private KafkaMessageEncoder<K, V> messageEncoder = new KafkaMessageEncoder<K, V>();
    
    /**
     * 是否运行：默认不运行
     */
    private AtomicBoolean running = new AtomicBoolean(false);
    
    /**
     * 资源
     */
    private Resource resource;
    
    /**
     * 发送客户端
     */
    private IKafkaMsSenderClient<byte[], byte[]> sender;
    
    /**
     * 构造方法
     * @param resource
     */
    public KafkaSenderMQ(Resource resource) {
        super();
        setResource(resource);
        setSender(getSender());
        logger.info("Message Sender MQ initialized.");
    }

    /**
     * 构造方法
     * @param resource
     * @param sender
     */
    public KafkaSenderMQ(Resource resource, IKafkaMsSenderClient<byte[], byte[]> sender) {
        super();
        setResource(resource);
        setSender(sender);
        logger.info("Message Sender MQ initialized.");
    }

    @Override
    public synchronized boolean isRunning() {
        return running.get();
    }

    @Override
    public synchronized void shutdown() {
        this.running.set(false);
    }

    @Override
    public void commitTransaction() {
        getSender().commitTransaction();
    }

    @Override
    public void beginTransaction() {
        getSender().beginTransaction();
    }

    @Override
    public void rollback() {
        getSender().rollback();
    }

    @Override
    public synchronized void destroy() {
        if(sender != null){
            sender.shutDown();
        }
        running.set(false);
        logger.info("Message Sender MQ destroyed.");
    }

    @Override
    public void setAutoCommitTransaction(boolean autoCommitTransaction) {
        getSender().setAutoCommitTransaction(autoCommitTransaction);
    }

    @Override
    public boolean getAutoCommitTransaction() {
        return getSender().getAutoCommitTransaction();
    }

    public Resource getResource() {
        return resource;
    }

    private void setResource(Resource resource) {
        this.resource = resource;
        
        try {
            PropertiesLoaderUtils.fillProperties(this.props, this.resource);
            Resource kafkaServersConfig = new DefaultResourceLoader().getResource("kafka/kafka-servers.properties");
            PropertiesLoaderUtils.fillProperties(props, kafkaServersConfig);
        } catch (IOException e) {
            logger.error("Fill properties failed.", e);
        }
    }

    public IKafkaMsSenderClient<byte[], byte[]> getSender() {
        if(sender == null){
            sender = new KafkaMsSenderClient<byte[], byte[]>(props);
        }
        return sender;
    }

    public void setSender(IKafkaMsSenderClient<byte[], byte[]> sender) {
        this.sender = sender;
    }

    @Override
    public void send(KafkaTopic topic, V message) throws KafkaException, KafkaUnrecoverableException, MQException {
        byte[] messageBytes = messageEncoder.encodeVal(message);
        getSender().send(topic.getTopic(), messageBytes);
    }

    @Override
    public void send(KafkaTopic topic, K key, V message) throws KafkaException, KafkaUnrecoverableException, MQException {
        byte[] keyBytes = messageEncoder.encodeKey(key);
        byte[] messageBytes = messageEncoder.encodeVal(message);
        getSender().sendMap(topic.getTopic(), keyBytes, messageBytes);
    }

    @Override
    public void send(KafkaTopic topic, V message, IKafkaCallback callback) throws MQException {
        byte[] messageBytes = messageEncoder.encodeVal(message);
        getSender().send(topic.getTopic(), messageBytes, callback);
    }

    @Override
    public void send(KafkaTopic topic, K key, V message, IKafkaCallback callback) throws MQException {
        byte[] keyBytes = messageEncoder.encodeKey(key);
        byte[] messageBytes = messageEncoder.encodeVal(message);
        getSender().sendMap(topic.getTopic(), keyBytes, messageBytes, callback);
    }
}
