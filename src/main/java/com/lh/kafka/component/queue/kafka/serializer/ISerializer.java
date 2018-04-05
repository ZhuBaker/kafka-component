package com.lh.kafka.component.queue.kafka.serializer;
/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月2日 下午6:01:11
 * 说明：kakfa序列化接口
 */
public interface ISerializer {

    /**
     * 获取名字
     * @return
     */
    public String name();

    /**
     * 序列化
     * @param obj
     * @return
     * @throws Throwable
     */
    public byte[] serialize(Object obj) throws Throwable ;
    
    /**
     * 反序列化
     * @param bytes
     * @return
     * @throws Throwable
     */
    public Object deserialize(byte[] bytes) throws Throwable ;
}
