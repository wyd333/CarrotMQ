package com.example.mq.mqserver.core;

import com.example.mq.common.MqException;

/**
 * Created with IntelliJ IDEA.
 * Description: 实现交换机的转发功能，同时也借助这个类进行一些参数的合法性验证
 * User: 12569
 * Date: 2024-03-20
 * Time: 22:10
 */
public class Router {
    /**
     * routingKey 的构造规则:
     * 1.数字, 字母, 下划线
     * 2.使用 . 分割成若干部分
     * 3.允许存在 * 和 # 作为通配符, 但通配符只能作为独立的分段
     * @param bindingKey
     * @return
     */
    public boolean checkBindingKey(String bindingKey) {
        if(bindingKey.length() == 0) {
            // 合法的情况
            // 使用 DIRECT 或 FANOUT 时, bindingKey 是用不上的
            return true;
        }
        for (int i = 0; i < bindingKey.length(); i++) {
            char ch = bindingKey.charAt(i);

            if(ch >= 'A' && ch <= 'Z') {
                continue;
            }
            if(ch >= 'a' && ch <= 'z') {
                continue;
            }
            if(ch >= '0' && ch <= '9') {
                continue;
            }
            if(ch == '_' || ch == '.' || ch == '*' || ch == '#') {
                continue;
            }
            return false;
        }
        // 检查 * 或 # 是否是独立的部分
        // aaa.*.bbb  合法; aaa.*b.ccc  非法
        String[] words = bindingKey.split("\\.");
        for(String word : words) {
            // 检查 word 的长度 > 1, 且包含了 * 或 #, 则非法
            if(word.length() > 1 && (word.contains("*") || word.contains("#"))) {
                return false;
            }
        }
        // 约定一下通配符之间的相邻关系(不是 RabbitMQ 约定的)
        // aaa.#.#.bbb  非法
        // aaa.#.*.bbb  非法
        // aaa.*.#.bbb  非法
        // aaa.*.*.bbb  非法
        for (int i = 0; i < words.length-1; i++) {
            // 判定是否是连续两个 #
            if(words[i].equals("#") && words[i+1].equals("#")) {
                return false;
            }
            // #*
            if(words[i].equals("#") && words[i+1].equals("*")) {
                return false;
            }
            // *#
            if(words[i].equals("*") && words[i+1].equals("#")) {
                return false;
            }
            
        }


        return true;
    }

    /**
     * routingKey 的构造规则:
     * 1.数字, 字母, 下划线
     * 2.使用 . 分割成若干部分
     * @param routingKey
     * @return
     */
    public boolean checkRoutingKey(String routingKey) {
        if(routingKey.length() == 0) {
            // 空字符串也是合法的情况
            // 比如在使用 FANOUT 时, routingKey用不上, 就可以设为 ""
            return true;
        }
        for(int i = 0; i < routingKey.length(); i++) {
            char ch = routingKey.charAt(i);
            // 判定该字符是否是大写字符
            if(ch >= 'A' && ch <= 'Z') {
                continue;
            }
            // 判定该字符是否是小写字符
            if(ch >= 'a' && ch <= 'z') {
                continue;
            }
            // 判定该字符是否是数字
            if(ch >= '0' && ch <= '9') {
                continue;
            }
            // 判定是否是 _ 或 .
            if(ch == '_' || ch == '.') {
                continue;
            }
            // 上述情况之外的就是非法的情况
            return false;
        }
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


    // [测试用例]
    // binding key          routing key         result
    // aaa                  aaa                 true
    // aaa.bbb              aaa.bbb             true
    // aaa.bbb              aaa.bbb.ccc         false
    // aaa.bbb              aaa.ccc             false
    // aaa.bbb.ccc          aaa.bbb.ccc         true
    // aaa.*                aaa.bbb             true
    // aaa.*.bbb            aaa.bbb.ccc         false
    // *.aaa.bbb            aaa.bbb             false
    // #                    aaa.bbb.ccc         true
    // aaa.#                aaa.bbb             true
    // aaa.#                aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.ccc             true
    // aaa.#.ccc            aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.aaa.bbb.ccc     true
    // #.ccc                ccc                 true
    // #.ccc                aaa.bbb.ccc         true
    private boolean routeTopic(Binding binding, Message message) {
        // 先把这两个 key 进行切分
        String[] bindingTokens = binding.getBindingKey().split("\\.");
        String[] routingTokens = message.getRoutingKey().split("\\.");

        // 引入两个下标, 指向上述两个数组. 初始情况下都为 0
        int bindingIndex = 0;
        int routingIndex = 0;
        // 此处使用 while 更合适, 每次循环, 下标不一定就是 + 1, 不适合使用 for
        while (bindingIndex < bindingTokens.length && routingIndex < routingTokens.length) {
            if (bindingTokens[bindingIndex].equals("*")) {
                // [情况二] 如果遇到 * , 直接进入下一轮. * 可以匹配到任意一个部分
                bindingIndex++;
                routingIndex++;
                continue;
            } else if (bindingTokens[bindingIndex].equals("#")) {
                // 如果遇到 #, 需要先看看有没有下一个位置
                bindingIndex++;
                if (bindingIndex == bindingTokens.length) {
                    // [情况三] 该 # 后面没东西了, 说明此时一定能匹配成功
                    return true;
                }

                // [情况四] # 后面还有东西, 拿着这个内容去 routingKey 中往后找, 找到对应的位置
                // findNextMatch 这个方法用来查找该部分在 routingKey 的位置并返回该下标, 没找到就返回 -1
                routingIndex = findNextMatch(routingTokens, routingIndex, bindingTokens[bindingIndex]);
                if (routingIndex == -1) {
                    // 没找到匹配的结果. 匹配失败
                    return false;
                }
                // 找到的匹配的情况, 继续往后匹配
                bindingIndex++;
                routingIndex++;
            } else {
                // [情况一] 如果遇到普通字符串, 要求两边的内容是一样的
                if (!bindingTokens[bindingIndex].equals(routingTokens[routingIndex])) {
                    return false;
                }
                bindingIndex++;
                routingIndex++;
            }
        }

        // [情况五] 判定是否是双方同时到达末尾
        // 比如 aaa.bbb.ccc  和  aaa.bbb 是要匹配失败的
        if (bindingIndex == bindingTokens.length && routingIndex == routingTokens.length) {
            return true;
        }
        return false;
    }

    private int findNextMatch(String[] routingTokens, int routingIndex, String bindingToken) {
        for (int i = routingIndex; i < routingTokens.length; i++) {
            if (routingTokens[i].equals(bindingToken)) {
                return i;
            }
        }
        return -1;
    }
}
