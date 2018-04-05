package com.lh.kafka.component.queue.kafka.support;

import java.io.Serializable;

/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月30日 下午2:27:52
 * 说明：kafka的topic
 */
public class KafkaTopic implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1270860077798680135L;

    private String topic;

    public KafkaTopic() {
        super();
    }
    
    public KafkaTopic(String topic) {
        super();
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
