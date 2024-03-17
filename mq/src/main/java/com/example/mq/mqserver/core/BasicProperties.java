package com.example.mq.mqserver.core;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * Description: Message的一部分，保存了Message对象的重要属性
 * User: 12569
 * Date: 2023-12-13
 * Time: 20:54
 */
public class BasicProperties implements Serializable {
    //消息的唯一身份标识
    //此处为了保证id的唯一性，使用 UUID 来作为message id
    //UUID：编程中用来生成唯一id的算法
    private String messageId;
    //是一个消息上带有的内容，和bindingKey相匹配
    //如果当前交换机类型是DIRECT，此时的routingKey就表示要转发的队列名
    //如果当前交换机类型是FANOUT，此时的routingKey就无意义（不使用）
    //如果当前交换机类型是TOPIC，此时的routingKey就要和bindingKey做匹配，符合要求的才能转发给对应队列
    private String routingKey;

    //表示消息是否要持久化， 1 不持久化；2 持久化 （和 RabbitMQ 一致）
    private int deliverMode = 1;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public int getDeliverMode() {
        return deliverMode;
    }

    public void setDeliverMode(int deliverMode) {
        this.deliverMode = deliverMode;
    }

    //针对 RabbitMQ 的 BasicProperties 还有很多属性，这里先不实现
}
