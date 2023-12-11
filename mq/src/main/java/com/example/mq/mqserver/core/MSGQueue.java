package com.example.mq.mqserver.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 存储消息的队列
 * User: 12569
 * Date: 2023-11-21
 * Time: 22:17
 */
public class MSGQueue {
    // 表示队列的身份标识
    private String name;
    // 表示队列是否持久化 true-持久化保存  false-不持久化
    private boolean durable = false;
    //如果为true，表明这个队列只能被一个消费者使用，别人用不了  false则是大家都能使用
    //保留字段，具体的功能暂不实现
    private boolean exclusive = false;
    //保留字段，自动删除功能，暂不实现。
    //true-没有消费者使用后就自动删除  false-不自动删除
    private boolean autoDelete = false;
    //扩展参数，先列在这里，暂时不实现
    private Map<String,Object> arguments = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }
}
