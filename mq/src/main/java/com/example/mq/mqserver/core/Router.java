package com.example.mq.mqserver.core;

import com.example.mq.common.MqException;

/**
 * Created with IntelliJ IDEA.
 * Description: 实现交换机的转发功能，同时也借助这个类验证 bindingKey 是否合法
 * User: 12569
 * Date: 2024-03-20
 * Time: 22:10
 */
public class Router {
    public boolean checkBindingKey(String bindingKey) {
        // TODO
        return true;
    }

    public boolean checkRoutingKey(String routingKey) {
        // TODO
        return true;
    }

    /**
     * 用来判定该消息是否可以转发给这个绑定的队列
     * @param exchangeType
     * @param binding
     * @param message
     * @return
     */
    public boolean route(ExchangeType exchangeType, Binding binding, Message message) throws MqException {
        // 根据不同的 exchangeType 使用不同的判定转发规则
        if(exchangeType == ExchangeType.FANOUT) {
            // FANOUT 类型, 交换机上绑定的所有队列都要转发
            return true;
        }else if(exchangeType == ExchangeType.TOPIC) {
            // TOPIC 类型, 规则会更复杂, 单独封装一个方法来实现
            return routeTopic(binding, message);
        }else{
            // 其它情况不应该存在
            throw new MqException("[Router] 交换机类型非法! exchangeType=" + exchangeType);
        }
    }

    private boolean routeTopic(Binding binding, Message message) {
        return true;
    }
}
