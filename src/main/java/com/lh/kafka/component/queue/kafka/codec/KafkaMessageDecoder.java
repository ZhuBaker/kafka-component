package com.lh.kafka.component.queue.kafka.codec;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.serializer.SerializationUtils;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午2:22:42
 * 说明：kafka解码器
 */
public class KafkaMessageDecoder<K, V> {

    /**
     * 解码K
     * @param bytes
     * @return
     * @throws MQException
     */
    @SuppressWarnings("unchecked")
    public K decodeKey(byte[] bytes) throws MQException {
        if (bytes != null)
            return (K) SerializationUtils.deserialize(bytes);
        return null;
    }
    
    /**
     * 解码V
     * @param bytes
     * @return
     * @throws MQException
     */
    @SuppressWarnings("unchecked")
    public V decodeVal(byte[] bytes) throws MQException {
        if (bytes != null)
            return (V) SerializationUtils.deserialize(bytes);
        return null;
    }
    
    /**
     * 解码map
     * @param bytes
     * @return
     * @throws MQException
     */
    public Map<K, V> decodeMap(Map<byte[], byte[]> bytes)
            throws MQException {
        Map<K, V> map = new HashMap<K, V>();
        if (bytes != null)
            for (Entry<byte[], byte[]> entry : bytes.entrySet())
                map.put(decodeKey(entry.getKey()), decodeVal(entry.getValue()));

        return map;
    }
}
