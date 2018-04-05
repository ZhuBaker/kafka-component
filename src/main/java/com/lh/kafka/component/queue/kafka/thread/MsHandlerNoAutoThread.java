package com.lh.kafka.component.queue.kafka.thread;

import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lh.kafka.component.queue.kafka.adapter.KafkaMessageAdapter;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月4日 下午6:15:39
 * 说明：
 */
public class MsHandlerNoAutoThread<K, V> implements Runnable {

    public MsHandlerNoAutoThread(KafkaMessageAdapter<K, V> messageAdapter,
            BlockingQueue<ConsumerRecord<K, V>> blockingQueue) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        
    }

}
