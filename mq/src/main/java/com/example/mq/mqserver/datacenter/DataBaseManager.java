package com.example.mq.mqserver.datacenter;

import com.example.mq.MqApplication;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.mapper.MetaMapper;

import java.io.File;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description: 通过这个类来整合数据库操作，即dao
 * User: 12569
 * Date: 2024-02-07
 * Time: 21:50
 */
public class DataBaseManager {
    // 要从 Spring 中拿到现成的对象
    private MetaMapper metaMapper;

    /**
     * 针对数据库进行初始化
     */
    public void init(){
        // 手动获取到 MetaMapper 对象
        metaMapper = MqApplication.context.getBean(MetaMapper.class);
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

    public void deleteDB(){
        File file = new File("./data/meta.db");
        if(file.delete()){
            System.out.println("[DataBaseManager] 删除数据库文件成功！");
        }else{
            System.out.println("[DataBaseManager] 删除数据库文件失败！");
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

    // 封装其他的数据库操作
    public void insertExchange(Exchange exchange) {
        metaMapper.insertExchange(exchange);
    }

    public List<Exchange> selectAllExchanges() {
        return metaMapper.selectAllExchanges();
    }


    public void deleteExchange(String exchangeName) {
        metaMapper.deleteExchange(exchangeName);
    }

    public void insertQueue(MSGQueue queue) {
        metaMapper.insertQueue(queue);
    }

    public List<MSGQueue> selectAllQueues() {
        return metaMapper.selectAllQueues();
    }


    public void deleteQueue(String queueName) {
        metaMapper.deleteQueue(queueName);
    }

    public void insertBinding(Binding binding) {
        metaMapper.insertBinding(binding);
    }

    public List<Binding> selectAllBindings() {
        return metaMapper.selectAllBindings();
    }


    public void deleteBinding(Binding binding) {
        metaMapper.deleteBinding(binding);
    }
}
