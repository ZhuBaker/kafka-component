package com.lh.kafka.component.queue.kafka;

import com.lh.kafka.component.queue.MQueue;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午10:36:09
 * 说明：kafka消息队列接口
 */
public interface IKafakaMQ extends MQueue {

    /**
     * 是否正在运行中
     * 
     * @return
     */
    public boolean isRunning();

    /**
     * 关闭
     */
    public void shutdown();
}
