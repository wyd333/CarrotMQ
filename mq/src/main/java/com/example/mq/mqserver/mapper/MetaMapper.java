package com.example.mq.mqserver.mapper;

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

}
