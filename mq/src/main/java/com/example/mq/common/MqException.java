package com.example.mq.common;

/**
 * Created with IntelliJ IDEA.
 * Description: 自定义异常，用于 mq 的业务逻辑中
 * User: 12569
 * Date: 2024-03-11
 * Time: 21:40
 */
public class MqException extends Exception{
    public MqException(String reason) {
        super(reason);
    }
}
