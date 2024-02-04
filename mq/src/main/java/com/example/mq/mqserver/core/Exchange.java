package com.example.mq.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    /**
     * 把当前的arguments参数从Map转成String(JSON)；
     * 若代码出现异常，则返回空的json字符串。
     * @return
     */
    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "{}";
    }

    /**
     * 把参数argumentsJson按照json格式解析，转成Map对象；
     * 从数据库读取数据后构造Exchange对象时自动调用。
     * @param argumentsJson 从数据库中读取到的json字符串
     */
    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // 第二个参数用来描述当前的json字符串要转换成的java对象是什么类型的。
            // 如果是简单类型，直接使用对应类型的类对象；
            // 如果是集合类型这样的复杂类型，可以使用TypeReference匿名内部类对象来描述复杂类型的具体信息（通过泛型来描述）。
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String, Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
