package com.lh.kafka.component.queue.kafka.serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lh.kafka.component.queue.exception.MQException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月2日 下午5:59:36
 * 说明：序列化工具
 */
public class SerializationUtils {

    private final static Logger logger = LoggerFactory.getLogger(SerializationUtils.class);
    
    /**
     * kafka序列化接口
     */
    private static ISerializer serializer;

    static {
        String ser = SerializationConfig.getSerializer();
        if (ser == null || "".equals(ser.trim())){
            serializer = new JavaSerializer();
        }else {
            if (ser.equals("java")) {
                serializer = new JavaSerializer();
            } else if (ser.equals("fst")) {
                serializer = new FSTSerializer();
            } else if (ser.equals("kryo")) {
                serializer = new KryoSerializer();
            } else if (ser.equals("kryo_pool_ser")){
                serializer = new KryoPoolSerializer();
            } else {
                try {
                    serializer = (ISerializer) Class.forName(ser).newInstance();
                } catch (Throwable e) {
                    logger.error("Cannot initialize Serializer name [" + ser + ']', e);
                }
            }
        }
        logger.info("Using Serializer name [" + serializer.name() + ":" + serializer.getClass().getName() + ']');
    }

    /**
     * 序列化
     * @param obj
     * @return
     * @throws MQException
     */
    public static byte[] serialize(Object obj) throws MQException {
        try{
            return serializer.serialize(obj);
        }catch(Throwable e){
            throw new MQException(e);
        }
    }

    /**
     * 反序列化
     * @param bytes
     * @return
     * @throws MQException
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] bytes) throws MQException {
        try{
             return (T) serializer.deserialize(bytes);
        }catch(Throwable e){
            throw new MQException(e);
        }
    }
}
