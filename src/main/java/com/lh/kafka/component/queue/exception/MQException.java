package com.lh.kafka.component.queue.exception;
/**
 * @author 林浩<hao.lin@w-oasis.com>
 * @version 创建时间：2018年3月29日 上午10:58:45
 * 说明：自定应消息对象异常
 */
public class MQException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -135892748022862790L;

    public MQException() {
        super();
    }

    public MQException(String message) {
        super(message);
    }

    public MQException(Throwable cause) {
        super(cause);
    }

    public MQException(String message, Throwable cause) {
        super(message, cause);
    }
}
