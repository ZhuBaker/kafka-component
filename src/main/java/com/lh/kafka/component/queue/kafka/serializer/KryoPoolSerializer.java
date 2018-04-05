package com.lh.kafka.component.queue.kafka.serializer;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.lh.kafka.component.queue.exception.MQException;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月2日 下午6:13:09
 * 说明：Kryo连接池（借助队列）
 */
public class KryoPoolSerializer implements ISerializer {

    @Override
    public String name() {
        return "kryo_pool_ser";
    }

    @Override
    public byte[] serialize(Object obj) throws Throwable {
        KryoHolder kryoHolder = null;
        if (obj == null) {
            throw new MQException("Param obj can not be null.");
        }
        
        try {
            kryoHolder = KryoPool.getInstance().get();
            //clear Output-->每次调用的时候  重置
            kryoHolder.output.clear(); 
            kryoHolder.kryo.writeClassAndObject(kryoHolder.output, obj);
            //无法避免拷贝 
            return kryoHolder.output.toBytes();
        } catch (Throwable e) {
            throw new MQException("Serialize obj exception.");
        } finally {
            KryoPool.getInstance().offer(kryoHolder);
            obj = null; 
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws Throwable {
        KryoHolder kryoHolder = null;
        if (bytes == null) {
            throw new MQException("Param obj can not be null.");
        }
        
        try {
            kryoHolder = KryoPool.getInstance().get();
            kryoHolder.input.setBuffer(bytes, 0, bytes.length);//call it ,and then use input object  ,discard any array
            return kryoHolder.kryo.readClassAndObject(kryoHolder.input);
        } catch (Throwable e) {
            throw new MQException("Deserialize bytes exception");
        } finally {
            KryoPool.getInstance().offer(kryoHolder);
            bytes = null;
        }
    }
    
    /**
     * Kryo池
     */
    private static class KryoPool {
        /**双端队列:放回去（队尾），出队（队头）*/
        private final Deque<KryoHolder> kryoHolderDeque = new ConcurrentLinkedDeque<KryoHolder>();

        public static KryoPool getInstance() {
            return Instance.pool;
        }
        
        private KryoPool(){}

        public void offer(KryoHolder kryoHolder) {
            kryoHolderDeque.addLast(kryoHolder);
        }

        public KryoHolder get() {
            KryoHolder kryoHolder = kryoHolderDeque.pollFirst();
            if(kryoHolder == null){
                kryoHolder = createKryoHolder();
            }
            return kryoHolder;
        }

        private KryoHolder createKryoHolder() {
            Kryo kryo = new Kryo();
            kryo.setReferences(false);
            return new KryoHolder(kryo);
        }

        /**内部单例类*/
        private static class Instance {
            private static final KryoPool pool = new KryoPool();
        }
    }

    /**
     * Kryo 的包装
     */
    private static class KryoHolder {
        static final int BUFFER_SIZE = 1024;
      
        //reuse
        private Output output = new Output(BUFFER_SIZE, -1);
        private Input input = new Input();

        private Kryo kryo;
        
        KryoHolder(Kryo kryo) {
            this.kryo = kryo;
        }
    }
}
