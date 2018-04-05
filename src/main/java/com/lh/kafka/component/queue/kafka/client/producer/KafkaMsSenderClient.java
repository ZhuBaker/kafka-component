package com.lh.kafka.component.queue.kafka.client.producer;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lh.kafka.component.queue.kafka.IKafkaCallback;
import com.lh.kafka.component.queue.kafka.exception.KafkaUnrecoverableException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 下午3:32:02
 * 说明：kafka消息生产者实现类
 */
public class KafkaMsSenderClient<K, V> extends KafkaProducer<K, V> implements IKafkaMsSenderClient<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMsSenderClient.class);

    /** 是否自动提交事务*/
    private  boolean autoCommitTransaction = false;
    /** 是否初始化事务*/
    private  boolean isInitTransaction = false;

    /** kafka提交回调*/
    private Callback iKafkaCallback = new IKafkaCallback() {
        
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception != null){
                logger.error("Message send error.", exception);
            }else {
                
            }
        }
    };
    
    /**
     * 构造方法
     * @param configs
     * @param keySerializer
     * @param valueSerializer
     */
    public KafkaMsSenderClient(Map<String, Object> configs,
            Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    /**
     * 构造方法
     * @param configs
     */
    public KafkaMsSenderClient(Map<String, Object> configs) {
        super(configs);
    }

    /**
     * 构造方法
     * @param properties
     * @param keySerializer
     * @param valueSerializer
     */
    public KafkaMsSenderClient(Properties properties,
            Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }

    /**
     * 构造方法
     * @param properties
     */
    public KafkaMsSenderClient(Properties properties) {
        super(properties);
    }

    @Override
    public void send(String topic, V value) throws KafkaUnrecoverableException,
            KafkaException {
        try {
            this.send(new ProducerRecord<K, V>(topic, value));
            if(autoCommitTransaction){
                this.commitTransaction();
            }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            this.close();
            throw new KafkaUnrecoverableException("Message send exception", e);
        }catch (KafkaException e) {
            throw e;
        }
    }

    @Override
    public void send(String topic, V value, IKafkaCallback callback) {
        if (callback != null) {
            this.send(new ProducerRecord<K, V>(topic, value), callback);
        }else{
            this.send(new ProducerRecord<K, V>(topic, value), iKafkaCallback );
        }
    }

    @Override
    public void sendMap(String topic, K key, V value)
            throws KafkaUnrecoverableException, KafkaException {
        try {
            this.send(new ProducerRecord<K, V>(topic, key, value));
            if(autoCommitTransaction){
                this.commitTransaction();
            }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            this.close();
            throw new KafkaUnrecoverableException("Map message send exception", e);
        }catch (KafkaException e) {
            throw e;
        }
    }

    @Override
    public void sendMap(String topic, K key, V value, IKafkaCallback callback) {
        if (callback != null) {
            this.send(new ProducerRecord<K, V>(topic, key, value), callback);
        }else{
            this.send(new ProducerRecord<K, V>(topic, key, value), iKafkaCallback );
        }
    }

    @Override
    public void setAutoCommitTransaction(boolean autoCommitTransaction) {
        this.autoCommitTransaction = autoCommitTransaction;
    }

    @Override
    public boolean getAutoCommitTransaction() {
        return autoCommitTransaction;
    }

    @Override
    public void shutDown() {
        this.flush();
        this.close();
    }

    @Override
    public void rollback() {
        this.abortTransaction();
    }
    
    @Override
    public void commitTransaction() {
       super.commitTransaction();
       this.flush();
    }
    
    @Override
    public void beginTransaction() {
        if(!isInitTransaction){
            this.initTransactions();
            isInitTransaction = true;
        }
        
        super.beginTransaction();
    }
}
