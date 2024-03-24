package com.example.mq.common;

import java.io.Serializable;


/**
 * Created with IntelliJ IDEA.
 * Description: 这个类对应的 basicConsume 方法中还有一个参数是回调函数(表示如何来处理消息)
 * 这个回调函数是不能通过网络传输的, 站在 broker server 这边针对消息的处理回调, 其实是统一的 (把消息返回给客户端)
 * 客户端这边收到消息之后再在客户端自己这边执行一个用户自定义的回调就行了
 * 此时, 客户端也就不需要把自身的回调告诉给服务器了
 * 这个类就不需要 consumer 成员了
 * User: 12569
 * Date: 2024-03-24
 * Time: 17:01
 */
public class BasicConsumeArguments extends BasicArguments implements Serializable {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;


    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }
}
