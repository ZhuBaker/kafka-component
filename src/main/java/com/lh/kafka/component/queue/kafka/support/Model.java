package com.lh.kafka.component.queue.kafka.support;
/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午11:15:20
 * 说明：
 */
public enum Model {
    /**   
     * MODEL_1 : 数据接收与业务处理在同一线程中（并发取决于队列分区） 
     */
    MODEL_1,
    
    /**   
     * MODEL_2 : 接收线程与业务线程分离（异步处理数据）
     */
    MODEL_2
}
