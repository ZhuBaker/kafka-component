package com.lh.kafka.component.queue.kafka.serializer;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年4月2日 下午6:05:58
 * 说明：序列化管理工具
 */
public class SerializationConfig {
    private static final Logger logger = LoggerFactory.getLogger(SerializationConfig.class);
    
    /**
     * 属性文件对象
     */
    private static Properties props = new Properties();
    
    /**
     * 序列化方式
     */
    private static String serializer;
    
    /**
     * 获取序列化方式
     * @return
     */
    public static String getSerializer() {
        if(serializer == null){
            try {
                Resource resource = new DefaultResourceLoader()
                   .getResource("kafka/mq-config.properties");
                   PropertiesLoaderUtils.fillProperties(props, resource);
                   setSerializer(props.getProperty("messge.serialization"));
               } catch (IOException e) {
                   logger.error("Fill properties failed.", e);
               }
       }
       return serializer;
    }

    /**
     * 设置属性
     * @param property
     */
    private static void setSerializer(String serializer) {
        SerializationConfig.serializer = serializer;
    }
}
