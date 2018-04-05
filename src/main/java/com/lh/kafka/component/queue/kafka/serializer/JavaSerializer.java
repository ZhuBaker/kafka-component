package com.lh.kafka.component.queue.kafka.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.lh.kafka.component.queue.exception.MQException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月2日 下午6:11:10
 * 说明：
 */
public class JavaSerializer implements ISerializer {

    @Override
    public String name() {
        return "java";
    }

    @Override
    public byte[] serialize(Object obj) throws Throwable {
        ObjectOutputStream oos = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            return baos.toByteArray();
        } finally {
            if(oos != null)
            try {
                oos.close();
            } catch (IOException e) {
                
            }
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws Throwable {
        if(bytes == null || bytes.length == 0)
            return null;
        ObjectInputStream ois = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new MQException(e);
        } finally {
            if(ois != null)
            try {
                ois.close();
            } catch (IOException e) {
            }
        }
    }

}
