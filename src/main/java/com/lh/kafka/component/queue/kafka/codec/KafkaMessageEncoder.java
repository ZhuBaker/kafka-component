package com.lh.kafka.component.queue.kafka.codec;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lh.kafka.component.queue.exception.MQException;
import com.lh.kafka.component.queue.kafka.serializer.SerializationUtils;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月3日 下午2:47:31
 * 说明：kafka编码器
 */
public class KafkaMessageEncoder<K, V> {

    public byte[] encodeKey(K key) throws MQException {
        if (key != null)
            return SerializationUtils.serialize(key);
        return null;
    }

    public byte[] encodeVal(V val) throws MQException {
        byte[] value = SerializationUtils.serialize(val);
        return value;
    }

    public Map<byte[], byte[]> encodeMap(Map<K, V> messages) throws MQException {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        if (messages != null){
            for (Entry<K, V> entry : messages.entrySet()){
                map.put(encodeKey(entry.getKey()), encodeVal(entry.getValue()));
            }
        }

        return map;
    }
}
