package com.lh.kafka.component.queue.kafka.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

import com.lh.kafka.component.queue.exception.MQException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月2日 下午6:11:23
 * 说明：
 */
public class FSTSerializer implements ISerializer {

    @Override
    public String name() {
        return "fst";
    }

    @Override
    public byte[] serialize(Object obj) throws Throwable {
        ByteArrayOutputStream out = null;
        FSTObjectOutput fout = null;
        try {
            out = new ByteArrayOutputStream();
            fout = new FSTObjectOutput(out);
            fout.writeObject(obj);
            fout.flush();
            return out.toByteArray();
        } finally {
            if(fout != null)
            try {
                fout.close();
            } catch (IOException e) {
            }
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws Throwable {
        if(bytes == null || bytes.length == 0)
            return null;
        FSTObjectInput in = null;
        try {
            in = new FSTObjectInput(new ByteArrayInputStream(bytes));
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new MQException(e);
        } finally {
            if(in != null)
            try {
                in.close();
            } catch (IOException e) {
                
            }
        }
    }
}
