package com.example.mq.common;

/**
 * Created with IntelliJ IDEA.
 * Description: 表示一个消费者完整的执行环境
 * User: 12569
 * Date: 2024-03-23
 * Time: 17:06
 */
public class ConsumerEnv {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
    // 通过这个回调来处理收到的消息
    private Consumer consumer;

    public ConsumerEnv(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        this.consumerTag = consumerTag;
        this.queueName = queueName;
        this.autoAck = autoAck;
        this.consumer = consumer;
    }

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

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }
}
