package com.example.mq.mqserver.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 交换机
 * User: 12569
 * Date: 2023-11-21
 * Time: 22:17
 */
public class Exchange {
    //使用name作为交换机的身份标识，唯一
    private String name;
    //交换机的类型 DIRECT FANOUT TOPIC
    private ExchangeType exchangeType = ExchangeType.DIRECT;
    //该交换机是否要持久化存储，true-需要，false-不需要
    private boolean durable = false;
    //如果当前交换机没人使用，就会自动删除(RabbitMQ有，但项目里没有实现，作为保留)
    private boolean autoDelete = false;
    //arguments指创建交换机时指定的额外的参数选项(RabbitMQ有，但项目里没有实现，作为保留)git
    //为了把这个arguments存到数据库中，需要把Map转成json格式的字符串
    private Map<String,Object> arguments = new HashMap<>();


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
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
