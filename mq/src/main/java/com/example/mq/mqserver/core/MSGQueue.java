package com.example.mq.mqserver.core;

import com.example.mq.common.ConsumerEnv;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

    //当前队列有哪些消费者订阅了
    //消费者名单
    private List<ConsumerEnv> consumerEnvList = new ArrayList<>();

    //记录当前取到了第几个消费者, 以实现轮询操作
    private AtomicInteger consumerSeq = new AtomicInteger(0);

    //添加一个新的订阅者
    public void addConsumerEnv(ConsumerEnv consumerEnv) {
        consumerEnvList.add(consumerEnv);
    }

    //订阅者的删除暂时先不考虑


    /**
     * 挑选一个订阅者来处理当前的消息
     * 以轮询的方式
     * @return
     */
    public ConsumerEnv chooseConsumer() {
        if(consumerEnvList.size() == 0) {
            // 该队列没有消费者订阅
            return null;
        }

        // 计算当前要取的元素的下标
        int index = consumerSeq.get() % consumerEnvList.size();
        // 记录订阅者的序号自增
        consumerSeq.getAndIncrement();
        // 根据下标选取消费者
        return consumerEnvList.get(index);
    }

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


    /**
     * 把当前的arguments参数进行序列化
     * 若代码出现异常则返回空的json字符串
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
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String, Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(String key, Object value){
        arguments.put(key, value);
    }

    public void setArguments(Map<String, Object> arguments){
        this.arguments = arguments;
    }
}
