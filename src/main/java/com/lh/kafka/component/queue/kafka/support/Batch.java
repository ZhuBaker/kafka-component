package com.lh.kafka.component.queue.kafka.support;
/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午11:19:46
 * 说明：消费模式（是否批量消费）
 */
public enum Batch {
    /**   
     * BATCH : 批量处理
     */
    YES,
    /**   
     * NO BATCH : 非批量处理（单个处理）
     */
    NO
}
