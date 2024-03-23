package com.example.mq.mqserver.core;

import com.example.mq.common.Consumer;
import com.example.mq.common.ConsumerEnv;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.VirtualHost;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * Description: 这个类来实现消费消息的核心逻辑
 * User: 12569
 * Date: 2024-03-23
 * Time: 17:37
 */
public class ConsumerManager {
    // 持有上层的 VirtualHost 对象的引用, 用来操作数据
    private VirtualHost parent;

    // 指定一个线程池, 负责去执行具体的回调任务
    private ExecutorService workerPool = Executors.newFixedThreadPool(4);

    // 存放令牌的队列
    private BlockingQueue<String> tokenQueue = new LinkedBlockingQueue<>();

    // 扫描线程
    private Thread scannerThread = null;

    public ConsumerManager(VirtualHost p) {
        parent = p;

        scannerThread = new Thread(() -> {
            while (true) {
                try {
                    // 1. 拿到令牌
                    String queueName = tokenQueue.take();
                    // 2. 根据令牌, 找到队列
                    MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
                    if (queue == null) {
                        throw new MqException("[ConsumerManager] 取令牌后发现, 该队列名不存在! queueName=" + queueName);
                    }
                    // 3. 从这个队列中消费一个消息.
                    synchronized (queue) {
                        consumeMessage(queue);
                    }
                } catch (InterruptedException | MqException e) {
                    e.printStackTrace();
                }
            }
        });
        // 把线程设为后台线程.
        scannerThread.setDaemon(true);
        scannerThread.start();
    }

    /**
     * 这个方法的调用时机就是发送消息的时候
     * @param queueName
     * @throws InterruptedException
     */
    public void notifyConsume(String queueName) throws InterruptedException {
        tokenQueue.put(queueName);
    }

    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) throws MqException {
        // 找到对应的队列
        MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
        if (queue == null) {
            throw new MqException("[ConsumerManager] 队列不存在! queueName=" + queueName);
        }

        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
        synchronized (queue) {
            queue.addConsumerEnv(consumerEnv);
            // 如果当前队列中已经有了一些消息, 需要立即就消费掉
            int n = parent.getMemoryDataCenter().getMessageCount(queueName);
            for (int i = 0; i < n; i++) {
                // 这个方法调用一次就消费一条消息, 取出消息交给线程池
                consumeMessage(queue);
            }
        }
    }

    private void consumeMessage(MSGQueue queue) {
        // 1. 按照轮询的方式找个消费者出来
        ConsumerEnv luckyDog = queue.chooseConsumer();
        if (luckyDog == null) {
            // 当前队列没有消费者, 暂时不消费, 等后面有消费者出现再说
            return;
        }

        // 2. 从队列中取出一个消息
        Message message = parent.getMemoryDataCenter().pollMessage(queue.getName());
        if (message == null) {
            // 当前队列中还没有消息, 也不需要消费
            return;
        }

        // 3. 把消息带入到消费者的回调方法中, 丢给线程池执行
        workerPool.submit(() -> {
            try {
                // 1. 把消息放到待确认的集合中. 这个操作势必在执行回调之前
                parent.getMemoryDataCenter().addMessageWaitAck(queue.getName(), message);

                // 2. 真正执行回调操作
                luckyDog.getConsumer().handleDelivery(luckyDog.getConsumerTag(), message.getBasicProperties(),
                        message.getBody());

                // 3. 如果当前是 "自动应答" , 就可以直接把消息删除了
                //    如果当前是 "手动应答" , 则先不处理, 交给后续消费者调用 basicAck 方法来处理
                if (luckyDog.isAutoAck()) {
                    // 1) 删除硬盘上的消息
                    if (message.getDeliverMode() == 2) {
                        parent.getDiskDataCenter().deleteMessage(queue, message);
                    }
                    // 2) 删除上面的待确认集合中的消息
                    parent.getMemoryDataCenter().removeMessageWaitAck(queue.getName(), message.getMessageId());
                    // 3) 删除内存中消息中心里的消息
                    parent.getMemoryDataCenter().removeMessage(message.getMessageId());
                    System.out.println("[ConsumerManager] 消息被成功消费! queueName=" + queue.getName());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}
