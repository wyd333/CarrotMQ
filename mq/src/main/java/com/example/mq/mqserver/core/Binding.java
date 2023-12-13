package com.example.mq.mqserver.core;

/**
 * Created with IntelliJ IDEA.
 * Description: 队列和交换机之间的关联关系
 * User: 12569
 * Date: 2023-11-21
 * Time: 22:18
 */
public class Binding {
    // Exchange和MSGQueue都是以name为身份标识的
    // 一个Exchange和多个queue之间有关联，创建多个Binding对象即可
    private String exchangeName;
    private String queueName;
    private String bindingKey;

    //binding是依附于Exchange和queue的
    //持久化、自动删除等属性也依附于Exchange和queue二者，单独针对binding设置这些属性没有意义

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}
