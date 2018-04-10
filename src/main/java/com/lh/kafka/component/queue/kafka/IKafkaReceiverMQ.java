package com.lh.kafka.component.queue.kafka;

import com.lh.kafka.component.queue.kafka.support.Commit;


/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午10:41:28
 * 说明：
 */
public interface IKafkaReceiverMQ extends IKafakaMQ {

    /**
     * 启动接收器
     */
    public void start();

    /**
     * 销毁池
     */
    public void destroy();
    
    /**
     * 取得提交模式
     * @return
     */
    public Commit getCommit();
}
