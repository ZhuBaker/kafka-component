package com.lh.kafka.component.queue.kafka.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午2:55:24
 * 说明：线程工程
 */
public class KafkaThreadFactory implements ThreadFactory {
    
    private static final int MAX_PRIORITY = 10;
    
    private static final int MIN_PRIORITY = 1;
    
    private static AtomicInteger i = new AtomicInteger();

    /**
     * 线程名称前缀
     */
    private String namePrefix;
    
    /**
     * 线程优先级
     */
    private int priority;

    /**
     * 是否是守护线程
     */
    private boolean daemon;
    
    public KafkaThreadFactory(String namePrefix) {
        super();
        this.namePrefix = namePrefix;
    }

    public KafkaThreadFactory(String namePrefix, int priority) {
        super();
        this.namePrefix = namePrefix;
        this.priority = priority;
    }

    public KafkaThreadFactory(String namePrefix, int priority, boolean daemon) {
        super();
        this.namePrefix = namePrefix;
        this.priority = priority;
        this.daemon = daemon;
    }
    
    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        
        if(priority >= MIN_PRIORITY && priority <= MAX_PRIORITY){
            thread.setPriority(priority);
        }
        
        thread.setDaemon(daemon);
        if(namePrefix == null){
            namePrefix = "My-Kafka-Thread";
        }
        thread.setName(namePrefix + "-" + i.getAndIncrement());
        
        return thread;
    }

    public String getNamePrefix() {
        return namePrefix;
    }

    public int getPriority() {
        return priority;
    }

    public boolean isDaemon() {
        return daemon;
    }
}
