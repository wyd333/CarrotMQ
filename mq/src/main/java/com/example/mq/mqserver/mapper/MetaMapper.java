package com.example.mq.mqserver.mapper;

import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import org.apache.ibatis.annotations.Mapper;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 12569
 * Date: 2024-01-27
 * Time: 21:53
 */

@Mapper
public interface MetaMapper {
    //提供3个核心建表方法
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();

    //针对上述三个基本概念进行插入和删除
    void insertExchange(Exchange exchange);
    void deleteExchange(String exchangeName);
    void insertQueue(MSGQueue queue);
    void deleteQueue(String queueName);
    void insertBinding(Binding binding);
    void deleteBinding(Binding binding);


}
