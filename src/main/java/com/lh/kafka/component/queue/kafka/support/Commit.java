package com.lh.kafka.component.queue.kafka.support;
/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午11:12:53
 * 说明：提交模式
 */
public enum Commit {
    /**   
     * AUTO_COMMIT : 自动提交
     */
    AUTO_COMMIT,
    /**   
     * SYNC_COMMIT : 同步提交
     */
    SYNC_COMMIT,
    /**   
     * ASYNC_COMMIT : 异步提交 
     */
    ASYNC_COMMIT
}
