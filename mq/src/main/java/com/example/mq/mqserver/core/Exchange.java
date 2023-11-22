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
    private Map<String, Object> arguments = new HashMap<>();
}
