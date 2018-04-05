package com.lh.kafka.component.queue.kafka.exception;
/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 下午3:48:22
 * 说明：
 */
public class KafkaUnrecoverableException extends Throwable {

    /**
     * 
     */
    private static final long serialVersionUID = -3934680542791901969L;

    public KafkaUnrecoverableException() {
        super();
    }

    public KafkaUnrecoverableException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public KafkaUnrecoverableException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaUnrecoverableException(String message) {
        super(message);
    }

    public KafkaUnrecoverableException(Throwable cause) {
        super(cause);
    }
}
