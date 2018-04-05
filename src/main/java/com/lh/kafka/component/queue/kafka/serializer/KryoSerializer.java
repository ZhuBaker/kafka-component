package com.lh.kafka.component.queue.kafka.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月2日 下午6:13:02
 * 说明：
 */
public class KryoSerializer implements ISerializer {

    private Kryo kryo = new Kryo();

    @Override
    public String name() {
        return "kryo";
    }

    @Override
    public byte[] serialize(Object obj) throws Throwable {
        Output output = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            output = new Output(baos);
            kryo .writeClassAndObject(output, obj);
            output.flush();
            return baos.toByteArray();
        }finally{
            if(output != null)
                output.close();
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws Throwable {
        if(bytes == null || bytes.length == 0)
            return null;
        Input ois = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ois = new Input(bais);
            return kryo.readClassAndObject(ois);
        } finally {
            if(ois != null)
                ois.close();
        }
    }
}
