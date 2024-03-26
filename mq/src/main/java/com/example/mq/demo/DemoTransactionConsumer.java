package com.example.mq.demo;

import com.example.mq.common.BasicReturns;
import com.example.mq.common.Consumer;
import com.example.mq.common.MqException;
import com.example.mq.mqclient.Channel;
import com.example.mq.mqclient.Connection;
import com.example.mq.mqclient.ConnectionFactory;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;

import java.io.IOException;

public class DemoTransactionConsumer {
    public static void main(String[] args) throws IOException, MqException {
        System.out.println("启动消费者!");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        channel.queueDeclare("testQueue", true, false, false, null);

        // 消费消息并进行事务处理
        consumeAndHandleWithTransaction(channel);

        // 关闭频道和连接
        channel.close();
        connection.close();
    }

    private static void consumeAndHandleWithTransaction(Channel channel) throws IOException, MqException {
        // 开启事务模式
        channel.txSelect();

        // 消费消息并处理
        channel.basicConsume("testQueue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                try {
                    // 开始事务处理
                    System.out.println("[消费数据] 开始!");
                    System.out.println("consumerTag=" + consumerTag);
                    System.out.println("basicProperties=" + basicProperties);
                    String bodyString = new String(body, 0, body.length);
                    System.out.println("body=" + bodyString);
                    // 模拟处理消息
                    boolean processResult = processMessage(bodyString);
                    // 根据处理结果选择提交事务或者回滚事务
                    if (processResult) {
                        channel.txCommit();
                        System.out.println("Transaction committed.");
                    } else {
                        channel.txRollback();
                        System.out.println("Transaction rolled back.");
                    }
                } catch (Exception e) {
                    // 出现异常时回滚事务
                    channel.txRollback();
                    System.out.println("Transaction rolled back due to exception: " + e.getMessage());
                } finally {
                    System.out.println("[消费数据] 结束!");
                }
            }
        });
    }

    // 模拟消息处理逻辑
    private static boolean processMessage(String message) {
        // 模拟消息处理，根据业务逻辑返回处理结果
        return message.contains("important");
    }
}
