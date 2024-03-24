package com.example.mq.common;

import com.example.mq.mqserver.core.BasicProperties;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description: 函数式接口(回调), 收到消息之后要处理消息时要用到的方法
 * User: 12569
 * Date: 2024-03-23
 * Time: 16:37
 */

@FunctionalInterface
public interface Consumer {
    /**
     * 在每次服务器接收到消息之后调用这个方法, 通过这个方法把消息推送给对应的消费者
     * 此处的方法名和参数参考 Rabbitmq
     * @param consumerTag
     * @param basicProperties
     * @param body
     */
    void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException;
}
