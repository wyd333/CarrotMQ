package com.example.mq.mqserver;

import com.example.mq.common.Config;
import com.example.mq.common.Consumer;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.*;
import com.example.mq.mqserver.datacenter.DiskDataCenter;
import com.example.mq.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * Description: 虚拟主机
 * 每个主机下面都管理着自己的交换机，队列，绑定，消息，数据
 * 同时提供 api 供上层调用
 * 针对 VirtualHost 这个类, 作为业务逻辑的整合者, 就需要对于代码中抛出的异常进行处理
 * User: 12569
 * Date: 2024-03-20
 * Time: 21:14
 */
public class VirtualHost {
    private String virtualHostName;
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    private Router router = new Router();
    private ConsumerManager consumerManager = new ConsumerManager(this);

    // 操作交换机的锁对象
    private final Object exchangeLocker = new Object();
    // 操作队列的锁对象
    private final Object queueLocker = new Object();
    public String getVirtualHostName() {
        return virtualHostName;
    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }

    public VirtualHost(String name) {
        this.virtualHostName = name;

        // 对于 MemoryDataCenter 来说, 不需要额外的初始化操作的, 只要对象 new 出来就行了
        // 但是针对 DiskDataCenter 来说, 则需要进行初始化操作, 建库建表和初始数据的设定
        diskDataCenter.init();

        // 另外还需要针对硬盘的数据, 进行恢复到内存中
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | MqException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("[VirtualHost] 恢复内存数据失败!");
        }
    }

    /**
     * 创建交换机, 如果交换机不存在, 就创建; 如果存在, 直接返回
     * @param exchangeName
     * @param exchangeType
     * @param durable
     * @param autoDelete
     * @param arguments
     * @return 返回值是 boolean. 创建成功, 返回 true. 失败返回 false
     */
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete,
                                   Map<String, Object> arguments) {

        // 把交换机的名字加上虚拟主机作为前缀, 表示交换机和虚拟主机之间的从属关系
        // 虚拟主机的目的是为了保证隔离
        exchangeName = virtualHostName + exchangeName;
        try {
            synchronized (exchangeLocker) {
                // 1. 判定该交换机是否已经存在,直接通过内存查询
                Exchange existsExchange = memoryDataCenter.getExchange(exchangeName);
                if (existsExchange != null) {
                    // 该交换机已经存在!
                    System.out.println("[VirtualHost] 交换机已经存在! exchangeName=" + exchangeName);
                    return true;
                }

                // 2. 真正创建交换机, 先构造 Exchange 对象
                Exchange exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);

                // 3. 把交换机对象写入硬盘
                // 先写硬盘, 后写内存, 目的是因为硬盘更容易写失败. 如果硬盘写失败了, 内存就不写了
                // 要是先写内存, 内存写成功了而硬盘写失败了, 还需要把内存的数据给再删掉, 就比较麻烦了
                // 尽量考虑一些原子性
                if (durable) {
                    diskDataCenter.insertExchange(exchange);
                }

                // 4. 把交换机对象写入内存
                memoryDataCenter.insertExchange(exchange);
                System.out.println("[VirtualHost] 交换机创建完成! exchangeName=" + exchangeName);
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 交换机创建失败! exchangeName=" + exchangeName);
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 删除交换机
     * @param exchangeName
     * @return
     */
    public boolean exchangeDelete(String exchangeName) {
        exchangeName = virtualHostName + exchangeName;
        try {
            synchronized (exchangeLocker) {
                // 1. 先找到对应的交换机
                Exchange toDelete = memoryDataCenter.getExchange(exchangeName);
                if (toDelete == null) {
                    throw new MqException("[VirtualHost] 交换机不存在，无法删除!");
                }
                // 2. 删除硬盘上的数据
                if (toDelete.isDurable()) {
                    diskDataCenter.deleteExchange(exchangeName);
                }
                // 3. 删除内存中的交换机数据
                memoryDataCenter.deleteExchange(exchangeName);
                System.out.println("[VirtualHost] 交换机删除成功! exchangeName=" + exchangeName);
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 交换机删除失败! exchangeName=" + exchangeName);
            e.printStackTrace();
            return false;
        }
    }

    // 创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete,
                                Map<String, Object> arguments) {
        // 把队列的名字, 给拼接上虚拟主机的名字
        queueName = virtualHostName + queueName;
        try {
            synchronized (queueLocker) {
                // 1. 判定队列是否存在
                MSGQueue existsQueue = memoryDataCenter.getQueue(queueName);
                if (existsQueue != null) {
                    System.out.println("[VirtualHost] 队列已经存在! queueName=" + queueName);
                    return true;
                }

                // 2. 创建队列对象
                MSGQueue queue = new MSGQueue();
                queue.setName(queueName);
                queue.setDurable(durable);
                queue.setExclusive(exclusive);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);

                // 3. 写硬盘
                if (durable) {
                    diskDataCenter.insertQueue(queue);
                }
                // 4. 写内存
                memoryDataCenter.insertQueue(queue);
                System.out.println("[VirtualHost] 队列创建成功! queueName=" + queueName);
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 队列创建失败! queueName=" + queueName);
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 删除队列
     * @param queueName
     * @return
     */
    public boolean queueDelete(String queueName) {
        queueName = virtualHostName + queueName;
        try {
            synchronized (queueLocker) {
                // 1. 根据队列名字, 查询下当前的队列对象
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if (queue == null) {
                    throw new MqException("[VirtualHost] 队列不存在, 无法删除! queueName=" + queueName);
                }
                // 2. 删除硬盘数据
                if (queue.isDurable()) {
                    diskDataCenter.deleteQueue(queueName);
                }
                // 3. 删除内存数据
                memoryDataCenter.deleteQueue(queueName);
                System.out.println("[VirtualHost] 删除队列成功! queueName=" + queueName);
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 删除队列失败! queueName=" + queueName);
            e.printStackTrace();
            return false;
        }
    }

    public boolean queueBind(String queueName, String exchangeName, String bindingKey) {
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;

        try {
            synchronized (exchangeLocker) {
                synchronized (queueLocker) {
                    // 1. 判定当前的绑定是否已经存在了
                    Binding existsBinding = memoryDataCenter.getBinding(exchangeName, queueName);
                    if (existsBinding != null) {
                        throw new MqException("[VirtualHost] binding 已经存在! queueName=" + queueName
                                + ", exchangeName=" + exchangeName);
                    }

                    // 2. 验证 bindingKey 是否合法
                    if (!router.checkBindingKey(bindingKey)) {
                        throw new MqException("[VirtualHost] bindingKey 非法! bindingKey=" + bindingKey);
                    }

                    // 3. 创建 Binding 对象
                    Binding binding = new Binding();
                    binding.setExchangeName(exchangeName);
                    binding.setQueueName(queueName);
                    binding.setBindingKey(bindingKey);

                    // 4. 获取一下对应的交换机和队列
                    // 如果交换机或者队列不存在, 那这样的绑定也是无法创建的
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    if (queue == null) {
                        throw new MqException("[VirtualHost] 队列不存在! queueName=" + queueName);
                    }
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    if (exchange == null) {
                        throw new MqException("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
                    }

                    // 5. 先写硬盘
                    if (queue.isDurable() && exchange.isDurable()) {
                        diskDataCenter.insertBinding(binding);
                    }
                    // 6. 写入内存
                    memoryDataCenter.insertBinding(binding);
                }
            }
            System.out.println("[VirtualHost] 绑定创建成功! exchangeName=" + exchangeName
                    + ", queueName=" + queueName);
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 绑定创建失败! exchangeName=" + exchangeName
                    + ", queueName=" + queueName);
            e.printStackTrace();
            return false;
        }
    }

    public boolean queueUnbind(String queueName, String exchangeName) {
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;
        try {
            synchronized (exchangeLocker) {
                synchronized (queueLocker) {
                    // 1. 获取 binding 看是否已经存在
                    Binding binding = memoryDataCenter.getBinding(exchangeName, queueName);
                    if (binding == null) {
                        throw new MqException("[VirtualHost] 删除绑定失败! 绑定不存在! exchangeName=" + exchangeName + ", queueName=" + queueName);
                    }

                    // 2. 无论绑定是否持久化, 都尝试从硬盘删一下
                    // 就算不存在, 这个删除也无副作用
                    diskDataCenter.deleteBinding(binding);
                    // 3. 删除内存的数据
                    memoryDataCenter.deleteBinding(binding);
                    System.out.println("[VirtualHost] 删除绑定成功!");
                }
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 删除绑定失败!");
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 发送消息到指定的交换机/队列中
     * @param exchangeName
     * @param routingKey
     * @param basicProperties 消息的基本属性
     * @param body 消息体
     * @return
     */
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) {
        try {
            // 1. 转换交换机的名字
            exchangeName = virtualHostName + exchangeName;

            // 2. 检查 routingKey 是否合法
            if (!router.checkRoutingKey(routingKey)) {
                throw new MqException("[VirtualHost] routingKey 非法! routingKey=" + routingKey);
            }

            // 3. 查找交换机对象
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if (exchange == null) {
                throw new MqException("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
            }

            // 4. 判定交换机的类型
            if (exchange.getType() == ExchangeType.DIRECT) {
                // 按照直接交换机的方式来转发消息
                // 以 routingKey 作为队列的名字, 直接把消息写入指定的队列中
                // 此时, 可以无视绑定关系
                String queueName = virtualHostName + routingKey;

                // 5. 构造消息对象
                Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                // 6. 查找该队列名对应的对象
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if (queue == null) {
                    throw new MqException("[VirtualHost] 队列不存在! queueName=" + queueName);
                }
                
                // 7. 队列存在, 直接给队列中写入消息
                sendMessage(queue, message);
            } else {
                // 按照 fanout 和 topic 的方式来转发.
                // 5. 找到该交换机关联的所有绑定, 并遍历这些绑定对象
                ConcurrentHashMap<String, Binding> bindingsMap = memoryDataCenter.getBindings(exchangeName);
                for (Map.Entry<String, Binding> entry : bindingsMap.entrySet()) {
                    // 1) 获取到绑定对象, 判定对应的队列是否存在
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(binding.getQueueName());
                    if (queue == null) {
                        // 此处不抛出异常了, 因为可能此处有多个这样的队列, 希望不要因为一个队列的失败影响到其他队列的消息的传输
                        System.out.println("[VirtualHost] basicPublish 发送消息时, 发现队列不存在! queueName=" + binding.getQueueName());
                        continue;
                    }
                    // 2) 构造消息对象
                    Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                    // 3) 判定这个消息是否能转发给该队列
                    //    如果是 fanout, 所有绑定的队列都要转发的
                    //    如果是 topic, 还需要判定下 bindingKey 和 routingKey 是不是匹配
                    if (!router.route(exchange.getType(), binding, message)) {
                        continue;
                    }

                    // 4) 真正转发消息给队列
                    sendMessage(queue, message);
                }
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 消息发送失败!");
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 带自动重试机制的消息发布
     * @param exchangeName
     * @param routingKey
     * @param basicProperties
     * @param body
     * @return
     */
    public boolean basicPublishWithRetry(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) {
        return retryMessage(exchangeName, routingKey, basicProperties, body, Config.RETRY_COUNT);
    }

    /**
     * 消息重试
     * @param exchangeName
     * @param routingKey
     * @param basicProperties
     * @param body
     * @param retryCount
     * @return
     */
    private boolean retryMessage(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body, int retryCount) {
        try {
            // 1. 转换交换机的名字
            exchangeName = virtualHostName + exchangeName;

            // 2. 检查 routingKey 是否合法
            if (!router.checkRoutingKey(routingKey)) {
                throw new MqException("[VirtualHost] routingKey 非法! routingKey=" + routingKey);
            }

            // 3. 查找交换机对象
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if (exchange == null) {
                throw new MqException("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
            }

            // 4. 判定交换机的类型
            if (exchange.getType() == ExchangeType.DIRECT) {
                // 按照直接交换机的方式来转发消息
                // 以 routingKey 作为队列的名字, 直接把消息写入指定的队列中
                // 此时, 可以无视绑定关系
                String queueName = virtualHostName + routingKey;

                // 5. 构造消息对象
                Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                // 6. 查找该队列名对应的对象
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if (queue == null) {
                    throw new MqException("[VirtualHost] 队列不存在! queueName=" + queueName);
                }

                // 7. 队列存在, 直接给队列中写入消息
                sendMessage(queue, message);
            } else {
                // 按照 fanout 和 topic 的方式来转发.
                // 5. 找到该交换机关联的所有绑定, 并遍历这些绑定对象
                ConcurrentHashMap<String, Binding> bindingsMap = memoryDataCenter.getBindings(exchangeName);
                for (Map.Entry<String, Binding> entry : bindingsMap.entrySet()) {
                    // 1) 获取到绑定对象, 判定对应的队列是否存在
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(binding.getQueueName());
                    if (queue == null) {
                        // 此处不抛出异常了, 因为可能此处有多个这样的队列, 希望不要因为一个队列的失败影响到其他队列的消息的传输
                        System.out.println("[VirtualHost] basicPublish 发送消息时, 发现队列不存在! queueName=" + binding.getQueueName());
                        continue;
                    }
                    // 2) 构造消息对象
                    Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                    // 3) 判定这个消息是否能转发给该队列
                    //    如果是 fanout, 所有绑定的队列都要转发的
                    //    如果是 topic, 还需要判定下 bindingKey 和 routingKey 是不是匹配
                    if (!router.route(exchange.getType(), binding, message)) {
                        continue;
                    }

                    // 4) 真正转发消息给队列
                    sendMessage(queue, message);
                }
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 消息发送失败!");
            e.printStackTrace();
            if (retryCount < Config.MAX_RETRIES) {
                System.out.println("Retrying message (" + (retryCount + 1) + "/" + Config.MAX_RETRIES + ")...");
                try {
                    TimeUnit.SECONDS.sleep(Config.RETRY_INTERVAL_SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                return retryMessage(exchangeName, routingKey, basicProperties, body, retryCount + 1);
            } else {
                System.out.println("Max retries exceeded. Failed to process message.");
                return false;
            }
        }
    }

    private void sendMessage(MSGQueue queue, Message message) throws IOException, MqException, InterruptedException {
        // 此处发送消息就是把消息写入到 硬盘 和 内存 上
        int deliverMode = message.getDeliverMode();

        // deliverMode 为 1 , 不持久化
        // deliverMode 为 2 , 表示持久化
        if (deliverMode == 2) {
            diskDataCenter.sendMessage(queue, message);
        }
        // 写入内存
        memoryDataCenter.sendMessage(queue, message);

        // 此处还需要补充一个逻辑, 通知消费者可以消费消息了.
        consumerManager.notifyConsume(queue.getName());
    }


    /**
     * 订阅消息, 添加一个队列的订阅者, 当队列收到消息之后, 就要把消息推送给对应的订阅者
     * @param consumerTag 消费者的身份标识
     * @param queueName 订阅的是哪个队列
     * @param autoAck 消息被消费完成后应答的方式, 为 true 自动应答, 为 false 手动应答.
     * @param consumer 回调函数, 此处类型设定成函数式接口, 这样后续调用 basicConsume 并且传实参的时候就可以写作 lambda 样子了
     * @return
     */
    public boolean basicConsume(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        // 构造一个 ConsumerEnv 对象, 把这个对应的队列找到, 再把这个 Consumer 对象添加到该队列中
        queueName = virtualHostName + queueName;
        try {
            consumerManager.addConsumer(consumerTag, queueName, autoAck, consumer);
            System.out.println("[VirtualHost] basicConsume 成功! queueName=" + queueName);
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] basicConsume 失败! queueName=" + queueName);
            e.printStackTrace();
            return false;
        }
    }

    public boolean basicAck(String queueName, String messageId) {
        queueName = virtualHostName + queueName;
        try {
            // 1. 获取到消息和队列
            Message message = memoryDataCenter.getMessage(messageId);
            if (message == null) {
                throw new MqException("[VirtualHost] 要确认的消息不存在! messageId=" + messageId);
            }
            MSGQueue queue = memoryDataCenter.getQueue(queueName);
            if (queue == null) {
                throw new MqException("[VirtualHost] 要确认的队列不存在! queueName=" + queueName);
            }

            // 2. 删除硬盘上的数据
            if (message.getDeliverMode() == 2) {
                diskDataCenter.deleteMessage(queue, message);
            }
            // 3. 删除消息中心中的数据
            memoryDataCenter.removeMessage(messageId);
            // 4. 删除待确认的集合中的数据
            memoryDataCenter.removeMessageWaitAck(queueName, messageId);
            System.out.println("[VirtualHost] basicAck 成功! 消息被成功确认! queueName=" + queueName
                    + ", messageId=" + messageId);
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] basicAck 失败! 消息确认失败! queueName=" + queueName
                    + ", messageId=" + messageId);
            e.printStackTrace();
            return false;
        }
    }
}
