package com.example.mq.mqserver.datacenter;

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 12569
 * Date: 2024-03-18
 * Time: 23:09
 */

/*
 * 使用这个类来管理所有硬盘上的数据。
 * 1. 数据库: 交换机，绑定，队列
 * 2. 数据文件：消息
 * 上层逻辑如果需要操作硬盘，统一都通过这个类来使用。
 * （上层代码不关心当前数据是存储在数据库还是文件中的）
 */
public class DiskDataCenter {
    // 这个实例用来管理数据库中的数据
    private DataBaseManager dataBaseManager = new DataBaseManager();
    // 这个实例用来管理数据文件中的数据
    private MessageFileManager messageFileManager = new MessageFileManager();

    public void init() {
        // 针对上述两个实例进行初始化.
        dataBaseManager.init();
        // 当前 messageFileManager.init 是空的方法, 只是先列在这里, 一旦后续需要扩展, 就在这里进行初始化即可.
        messageFileManager.init();
    }

    // 封装交换机操作
    public void insertExchange(Exchange exchange) {
        dataBaseManager.insertExchange(exchange);
    }

    public void deleteExchange(String exchangeName) {
        dataBaseManager.deleteExchange(exchangeName);
    }

    public List<Exchange> selectAllExchanges() {
        return dataBaseManager.selectAllExchanges();
    }

    // 封装队列操作
    public void insertQueue(MSGQueue queue) throws IOException {
        dataBaseManager.insertQueue(queue);
        // 创建队列的同时, 不仅仅是把队列对象写到数据库中, 还需要创建出对应的目录和文件
        messageFileManager.createQueueFiles(queue.getName());
    }

    public void deleteQueue(String queueName) throws IOException {
        dataBaseManager.deleteQueue(queueName);
        // 删除队列的同时, 不仅仅是把队列从数据库中删除, 还需要删除对应的目录和文件
        messageFileManager.destroyQueueFiles(queueName);
    }

    public List<MSGQueue> selectAllQueues() {
        return dataBaseManager.selectAllQueues();
    }

    // 封装绑定操作
    public void insertBinding(Binding binding) {
        dataBaseManager.insertBinding(binding);
    }

    public void deleteBinding(Binding binding) {
        dataBaseManager.deleteBinding(binding);
    }

    public List<Binding> selectAllBindings() {
        return dataBaseManager.selectAllBindings();
    }


    // 封装消息操作
    public void sendMessage(MSGQueue queue, Message message) throws IOException, MqException {
        messageFileManager.sendMessage(queue, message);
    }

    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException, MqException {
        messageFileManager.deleteMessage(queue, message);
        // 判断是否需要GC
        if (messageFileManager.checkGC(queue.getName())) {
            messageFileManager.gc(queue);
        }
    }

    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, MqException, ClassNotFoundException {
        return messageFileManager.loadAllMessageFromQueue(queueName);
    }


}
