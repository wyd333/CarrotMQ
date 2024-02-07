package com.example.mq.mqserver.datacenter;

import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.mapper.MetaMapper;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * Description: 通过这个类来整合数据库操作，即dao
 * User: 12569
 * Date: 2024-02-07
 * Time: 21:50
 */
public class DataBaseManager {
    private MetaMapper metaMapper;

    /**
     * 针对数据库进行初始化
     */
    public void init(){
        if(!checkDBExists()){
            // 如果数据库不存在，则进行建库建表操作
            // 创建数据表
            createTable();
            // 插入默认数据
            createDefaultData();
            System.out.println("[DataBaseManager] 数据库初始化完成！");
        }else{
            // 数据库已经存在，则啥也不做
            System.out.println("[DataBaseManager] 数据库已存在！");
        }

    }


    private boolean checkDBExists() {
        File file = new File("./data/meta.db");
        return file.exists();
    }


    /**
     * 用于建表
     * 建库操作不用手动执行，即不用手动创建 meta.db 文件
     * 首次执行数据库操作的时候 MyBatis 会自动完成创建 meta.db 文件的操作
     */
    private void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createQueueTable();
        metaMapper.createBindingTable();
        System.out.println("[DataBaseManager] 创建表完成！");
    }

    /**
     * 给数据表中添加默认数据
     * 此处主要是添加一个交互机
     * （ RabbitMq 带有一个匿名的交换机，类型是 DIRECT）
     */
    private void createDefaultData() {
        // 构造一个默认的交换机
        Exchange exchange = new Exchange();
        exchange.setName("");
        exchange.setExchangeType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        metaMapper.insertExchange(exchange);
        System.out.println("[DataBaseManager] 创建初始数据完成！");
    }
}
