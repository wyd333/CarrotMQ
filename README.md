# CarrotMQ-消息队列

本项目为个人C/S项目，是一个基于Java语言编写的消息队列系统底层源码，实现并完善了消息队列MQ的业务功能，实现客户端与服务器跨主机的通信。该项目通过Spring Boot构建，以SQLite为数据库，以MyBatis实现对持久层的管理。

## 一、简介消息队列与CarrotMQ

在线程安全的集合类中有阻塞队列（Blocking Queue）的概念。本质上阻塞队列就是一个**进程内部的生产者消费者模型**。而所谓的消息队列就是把阻塞队列这样的数据结构单独提取成了一个程序，进行独立的部署。因此可以理解为，消息队列是一个**进程和进程之间或服务和服务之间（比如分布式系统）的生产者消费模型**。

生产者消费者模型有**解耦合**和**削峰填谷**这两大好处：

1. 解耦合体现在，本来在分布式系统中，若A服务器直接调用B服务器（A给B发请求，B给A返回响应），那么A和B之间的耦合是比较大的；而引入了消息队列后，A把请求发送到消息队列，B再从消息队列获取到请求，有了消息队列这个“中间人”，降低了服务器A和服务器B之间的耦合。
2. 削峰填谷体现在，当A是入口服务器时，A需调用B完成⼀些具体的业务，如果A和B直接通信，那么当A突然收到用户请求的峰值时，B也会随之收到峰值，对B的性能造成了很大的挑战。而B作为处理业务的核心服务器，对稳定性的要求是非常高的。当引入消息队列后，A把请求发送到消息队列，B再从消息队列获取到请求，此时A虽然收到了很多请求，但会先传递给队列，这样一来B仍旧可以按照原来的节奏来处理请求，不至于一下子就遭受太大的并发量而崩溃造成更严重的后果。 

当然，消息队列也应用于异步处理和日志处理。

当今市面上也有一些知名的消息队列，如RabbitMQ，Kafka，RocketMQ，ActiveMQ等。本项目是以RabbitMQ为原型搭建的，为了与之对应，故取名为CarrotMQ。

**搭建CarrotMQ这个消息队列项目的初心是本人公司在实习过程中接触到了消息队列相关的业务，对此颇感兴趣，于是在了解过一些资料后尝试手动实现。CarrotMQ中既有RabbitMQ的影子，也有博主本人自己的理解。此外，该项目将长期迭代，功能也会不断地完善（如增加图形化操作界面和增加更多功能）。**

值得说明的是，该项目在搭建过程中，完全按照工程上软件开发的流程，包括需求分析，制定计划，编码实现，软件测试等阶段。其中格外重视软件测试这一过程，核心业务逻辑均经过测试用例设计并已通过单元测试和集成测试。

项目结构见 ./illustration3.png。

## 二、核心功能

**1.发布消息功能：**生产者将消息发送给对应的交换机，交换机再根据不同的转发规则转发给与之相绑定且符合规则的消息队列。

**2.订阅消息功能：**消费者去某个消息队列处注册，表明自己要从该消息队列中取消息。

**3.消费消息功能：**当消息队列中有消息时，自动向该消息队列的订阅消费者推送消息。

## 三、项目亮点

从0到1编写消息队列源码，实现消息队列功能。如内存数据结构设计，模块划分，核心API的实现等，具体包括：

1.消息持久化。消息以文件的形式持久化至硬盘，当MQ启动时，可从硬盘上恢复数据至内存。

2.设计复制算法实现硬盘消息文件垃圾回收（GC）。

3.实现Topic交换机转发规则bindingKey与routingKey验证算法。

4.多线程操作及线程安全问题考虑。

5.基于TCP，自定义应用层协议实现网络通信。

#### 对比RabbitMQ，新增的功能点有：

**1.实现自动消息重试机制。**虽然RabbitMQ允许我们配置消息的重试策略，但是它没有内置的自动消息重试机制。CarrotMQ通过自定义的逻辑实现在消息处理失败时自动重试消息。

**2.内置的消息事务支持。**RabbitMQ提供了事务支持，但是这些事务是基于通道（channel）的，而不是基于消息的。CarrotMQ通过其它方案实现了多个消息之间执行的原子操作。

**3.消息流分析（仍在开发中）。**RabbitMQ没有内置的消息流分析功能，这可能会使在大规模消息系统中进行实时分析可能变得很复杂。虽然CarrotMQ暂时不会遇到大规模消息系统这样的体量，但CarrotMQ仍然希望进行尝试，增加内置的消息流分析功能来解决这个问题。

demo设计：

```java
public class MessageStreamAnalyzer {
    private MessageStreamCollector collector;
    private MessageStreamProcessor processor;
    private MessageStreamAnalyzerEngine analyzerEngine;
    private MessageStreamVisualizer visualizer;

    public MessageStreamAnalyzer() {
        // 初始化组件
        this.collector = new MessageStreamCollector();
        this.processor = new MessageStreamProcessor();
        this.analyzerEngine = new MessageStreamAnalyzerEngine();
        this.visualizer = new MessageStreamVisualizer();
    }

    public void analyzeMessageStream() {
        // 收集消息流数据
        MessageStreamData streamData = collector.collectMessageStreamData();

        // 处理消息流数据
        MessageStreamData processedData = processor.processMessageStreamData(streamData);

        // 分析消息流数据
        MessageStreamAnalysisResult analysisResult = analyzerEngine.analyzeMessageStream(processedData);

        // 可视化展示分析结果
        visualizer.visualizeMessageStreamAnalysisResult(analysisResult);
    }
}

public class MessageStreamCollector {
    public MessageStreamData collectMessageStreamData() {
        // 收集消息流数据的逻辑
        return new MessageStreamData();
    }
}

public class MessageStreamProcessor {
    public MessageStreamData processMessageStreamData(MessageStreamData streamData) {
        // 处理消息流数据的逻辑
        return streamData;
    }
}

public class MessageStreamAnalyzerEngine {
    public MessageStreamAnalysisResult analyzeMessageStream(MessageStreamData streamData) {
        // 分析消息流数据的逻辑
        return new MessageStreamAnalysisResult();
    }
}

public class MessageStreamVisualizer {
    public void visualizeMessageStreamAnalysisResult(MessageStreamAnalysisResult analysisResult) {
        // 可视化展示分析结果的逻辑
    }
}

public class MessageStreamData {
    // 消息流数据模型
}

public class MessageStreamAnalysisResult {
    // 消息流分析结果模型
}
```

**4.消息轨迹追踪（仍在开发中）。**RabbitMQ没有内置的消息轨迹追踪功能，不便于在消息流中追踪和调试。CarrotMQ也希望在这方面进行尝试，在某些情况下可能会借助外部工具或服务来实现这样的功能。

demo设计：

```java 
public class MessageProcessor {
    private MessageTracker messageTracker;

    public MessageProcessor(MessageTracker messageTracker) {
        this.messageTracker = messageTracker;
    }

    public void processMessage(Message message) {
        try {
            // 处理消息
            // ...

            // 记录消息轨迹
            messageTracker.logMessageProcessing(message, "Processed successfully");
        } catch (Exception e) {
            // 处理失败，记录错误信息
            messageTracker.logMessageProcessing(message, "Failed to process: " + e.getMessage());
        }
    }
}

public class MessageTracker {
    public void logMessageProcessing(Message message, String status) {
        // 记录消息的传递路径和处理状态
        // 可以将日志写入文件、数据库等
        System.out.println("Message ID: " + message.getId() + ", Status: " + status);
    }

    public String getMessageTrace(String messageId) {
        // 根据消息的唯一标识符查询消息的轨迹信息
        // 返回消息的传递路径和处理状态
        // 可以从日志文件、数据库中查询
        return "Message trace for ID " + messageId;
    }
}

public class Main {
    public static void main(String[] args) {
        // 创建消息追踪器
        MessageTracker messageTracker = new MessageTracker();

        // 创建消息处理器，并传入消息追踪器
        MessageProcessor messageProcessor = new MessageProcessor(messageTracker);

        // 模拟处理消息
        Message message = new Message("123", "Hello, world");
        messageProcessor.processMessage(message);

        // 查询消息轨迹信息
        String trace = messageTracker.getMessageTrace("123");
        System.out.println(trace);
    }
}
```

下面对各个模块分点进行介绍。

## 五、项目部分细节

### 1. 核心概念

CarrotMQ中涉及以下几个核心概念：

1.生产者（Producer） 。

2.消费者（Consumer） 。

3.中间人（Broker） 。

4.发布（Push），即生产者向中间人这里投递消息的过程。 

5.订阅（Subscribe） ，即记录了哪些消费者要从中间人这里取走数据。这个注册的过程称为“订阅” 。

6.消费 （Consume），即消费者从中间人这里取走数据的动作。

示意图1：一个生产者，一个消费者（见illustration1.png）

示意图2：N个生产者，N个消费者（见illustration2.png）

**同时，Broker Server（作用是管理如何进出队列）内部也涉及一些关键概念：**

1.虚拟主机（Virtual Host）。类似于MySQL中的database，是一个“逻辑”上的数据集合。 一个Broker Server上可以组织多种不同类别数据，可以使用Virtual Host做逻辑上的区分。 在实际开发中，一个Broker Server也可能同时用来管理多个业务线上的数据，也可以使用Virtual Host做逻辑上的区分。 

2.交换机（Exchange）。生产者把消息投递给Broker Server，实际上是把消息先交给了Broker Server上的交换机，再由交换机把消息交给对应的队列。CarrotMQ实现了**三种交换机类型**：

- Direct直接交换机：生产者发送消息时会指定⼀个“目标队列”的名字。交换机收到后，就在已绑定的队列里查找有无与之匹配的队列，如果有就转发过去（把消息塞进对应的队列中） ；如果没有，消息直接丢弃。
- Fanout 扇出交换机：会把消息放到交换机绑定的每个队列中，只要和这个交换机绑定任何队列都会转发消息。
- Topic 主题交换机： bindingKey，在把队列和交换机绑定的时候指定一个单词（类似于一个暗号）；routingKey，生产者发送消息的时候也指定一个单词。如果当前 bindingKey 和 routingKey 对上了，就可以把消息转发到对应的队列。

3.队列（Queue） 。真正用来存储处理消息的实体，后续消费者也是从对应的队列中取数据。 在一个大的消息队列中可以有很多具体的小队列。 

4.绑定（Binding）。即把交换机和队列之间建立关系，可以把交换机和队列视为数据库中多对多的关系。可以想象在MQ中也是有⼀个这样的中间表，所谓的“绑定”其实就是中间表中的一项。 

5.消息（Message）。具体指的是服务器A发给B的请求，以及服务器B给服务器A返回的响应。CarrotMQ中将一个消息视为一串二进制的数据。

### 2. Server核心API

消息队列服务器（Broker Server）提供的核心API有： 

1.创建队列（queueDeclare） 

2.销毁队列（queueDelete） 

3.创建交换机（exchangeDeclare） 

4.销毁交换机（exchageDelete） 

5.创建绑定（queueBind） 

6.解除绑定（queueUnbind） 

7.发布消息（basicPublish） 

8.订阅消息（basicConsume） 

9.确认消息（basicAck） 

### 3. 持久化机制

上述的虚拟机、交换机、队列、绑定、消息等均需存储。定义MessageFileManager类来负责管理消息在文件中的存储。具体实现的细节有：

1. 设计目录结构和文件格式 

2. 实现了目录创建和删除 

3. 实现统计文件的读写 

4. 实现了消息的写入（按照之前的文件格式） 

5. 实现了消息的删除 （随机访问文件） 

6. 实现了所有消息的加载 

7. 垃圾回收（复制算法）

存储思路是内存和硬盘各存一份，且**内存为主，硬盘为辅**。

**在内存中存储的原因：** 对于MQ来说，转发处理数据的效率是非常关键的指标。使用内存来组织数据，效率会比在硬盘中高很多。

**在硬盘中存储原因：** 防止内存中数据因进程重启/主机重启而丢失。 

实现如下： 

1.交换机、队列、绑定：存储在数据库中 。

2.消息：存储在文件中。 

### 4. 虚拟主机设计

类似于MySQL的database，为了把交换机、队列、绑定、消息等进行逻辑上的隔离，项目就设计了虚拟主机（VirtualHost）。一个服务器上可以有多个虚拟主机。在虚拟主机上将之前已经实现好了的各个核心API串起来。

### 5. 订阅和推送消息

一个虚拟主机中有很多队列，每个队列上都有很多条消息，那么针对是哪个消费者订阅了哪条队列的消息需要进行管理。消费者是以队列为维度来订阅消息的，一个队列可以有多个消费者（此处约定按照轮询的方式来进行消费）。

消息推送给消费者的基本实现思路：

1. 让 brokerserver把哪些消费者管理好。

2. 收到对应的消息，把消息推送给消费者 。

订阅消息的核心逻辑是调用consumerManager.addConsumer方法，并传入参数（consumerTag、queueName、autoAck、consumer【回调函数】）。 这个方法的底层是： 

1. 根据传入的queueName查到该队列。

2. 然后创建一个消费者表示ConsumerEnv，存入到该队列的ConsumerEnvList中。
3. 判断该队列中时候是否存在消息，若已经存在，就consumeMessage消费全部消息（按照轮询方式）。

```java
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

```

### 6.RPC通信

其他的服务器（生产者/消费者）需通过网络来与Broker Server进行交互。CarrotMQ使用TCP + 自定义的应用层协议来实现生产者/消费者和BrokerServer之间的通信。

此处应用层协议主要工作就是让客户端可以通过网络调用Broker Server提供的编程接口。 因此，客户端这边也将提供上述的核心API，不过客户端的API中只是发送/接受响应，并不真正处理业务逻辑（处理逻辑在Server中）。此处以RPC的思想来实现。

客户端与服务器的应用层通信协议示意图见 ./illumination4.png。具体字段有：

- type：描述当前这个请求和响应是做什么用的，用四个字节来存储。
  - 描述了在MQ中客户端（生产者 + 消费者）和 服务器 （Broker Server）之间要进行哪些操作，即当前这个请求/响应是在调用哪个API。 
  - 希望客户端，能通过网络远程调用这些API。
  - 不同的type表示请求/响应特定的功能，其取值是预定义的。
- length：存储payload的长度。占4个字节。
- payload：会根据当前是请求还是响应，以及当前的type取不同的值。
  - 如type是 0x3（表示创建交换机操作），同时当前是个请求，那么此时payload的内容就相当于是exchangeDelcare的参数的序列化结果。 
  - 如type是 0x3，同时当前是个响应，此时payload的内容，就相当于是exchangeDelcare的返回结果的序列化内容。

因此，客户端除了提供上述的核心API外，还需提供4个额外的方法以支撑其他工作： 

1. 创建 Connection 

2. 关闭 Connection
   - 此处用的是TCP连接。一个Connection对象就代表一个TCP连接。 

3. 创建 Channel 
   - 一个Connection里包含多个Channel，每个Channel上传输的数据都互不相干。
   - 在TCP中建立/断开⼀个连接成本是很高的，因此很多时候我们并不不希望频繁地建立/断开TCP连接，因此定义一个Channel，在不用的时候销毁 Channel。
   - 此处Channel是逻辑概念，比TCP轻量很多。 

4. 关闭 Channel 

## 六、Demo

启动服务器后，启动生产者与消费者客户端。

生产者demo：

```java
package com.example.mq.demo;

import com.example.mq.mqclient.Channel;
import com.example.mq.mqclient.Connection;
import com.example.mq.mqclient.ConnectionFactory;
import com.example.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/*
 * 这个类用来表示一个生产者
 */
public class DemoProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("启动生产者");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(8080);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 创建交换机和队列
        channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        channel.queueDeclare("testQueue", true, false, false, null);

        // 创建一个消息并发送
        byte[] body = "hello".getBytes();
        boolean ok = channel.basicPublish("testExchange", "testQueue", null, body);
        System.out.println("消息投递完成! ok=" + ok);

        Thread.sleep(500);
        channel.close();
        connection.close();
    }
}
```

消费者demo：

```java
package com.example.mq.demo;

import com.example.mq.common.Consumer;
import com.example.mq.common.MqException;
import com.example.mq.mqclient.Channel;
import com.example.mq.mqclient.Connection;
import com.example.mq.mqclient.ConnectionFactory;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/*
 * 这个类表示一个消费者
 */
public class DemoConsumer {
    public static void main(String[] args) throws IOException, MqException, InterruptedException {
        System.out.println("启动消费者!");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(8080);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        channel.queueDeclare("testQueue", true, false, false, null);

        channel.basicConsume("testQueue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                System.out.println("[消费数据] 开始!");
                System.out.println("consumerTag=" + consumerTag);
                System.out.println("basicProperties=" + basicProperties);
                String bodyString = new String(body, 0, body.length);
                System.out.println("body=" + bodyString);
                System.out.println("[消费数据] 结束!");
            }
        });

        // 由于消费者也不知道生产者要生产多少, 就在这里通过这个循环模拟一直等待消费
        while (true) {
            Thread.sleep(500);
        }
    }
}
```

## 七、部分项目亮点展示

### 1.实现自动消息重试机制

```java
public boolean basicPublishWithRetry(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) {
    return retryMessage(exchangeName, routingKey, basicProperties, body, Config.RETRY_COUNT);
}

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
```

### 2.内置的消息事务支持

```java
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
```

Channel中实现事务机制的代码：

```java
public class Channel {
    private String channelId;
    private Connection connection;
    private ConcurrentHashMap<String, BasicReturns> basicReturnsMap = new ConcurrentHashMap<>();
    private Consumer consumer = null;
    private boolean transactional = false;

    public Channel(String channelId, Connection connection) {
        this.channelId = channelId;
        this.connection = connection;
    }

    // 开启事务
    public void txSelect() {
        this.transactional = true;
    }

    // 提交事务
    public void txCommit() throws IOException {
        if (!transactional) {
            throw new IllegalStateException("Channel is not in transactional mode.");
        }
        // 发送提交事务的请求
        Request request = new Request();
        request.setType(0x10); // 假设0x10代表提交事务
        connection.writeRequest(request);
    }

    // 回滚事务
    public void txRollback() throws IOException {
        if (!transactional) {
            throw new IllegalStateException("Channel is not in transactional mode.");
        }
        // 发送回滚事务的请求
        Request request = new Request();
        request.setType(0x11); // 假设0x11代表回滚事务
        connection.writeRequest(request);
    }

    // 发送消息（支持事务）
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) throws IOException {
        BasicPublishArguments arguments = new BasicPublishArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setExchangeName(exchangeName);
        arguments.setRoutingKey(routingKey);
        arguments.setBasicProperties(basicProperties);
        arguments.setBody(body);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0x9);
        request.setLength(payload.length);
        request.setPayload(payload);

        if (!transactional) {
            // 非事务模式直接发送请求
            connection.writeRequest(request);
        } else {
            // 事务模式下将消息放入事务队列
            connection.addToTransactionQueue(request);
        }

        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }
    // 其他方法...
}
```

Connection.java

```java
// 将请求添加到事务队列中
public void addToTransactionQueue(Request request) {
    transactionQueue.add(request);
}

// 提交事务时，将事务队列中的请求发送给服务器
public void commitTransaction() throws IOException {
    // 构造事务请求
    Request transactionRequest = new Request();
    transactionRequest.setType(0x10); // 假设0x10代表提交事务
    transactionRequest.setLength(0);
    transactionRequest.setPayload(new byte[0]);

    // 发送事务请求
    writeRequest(transactionRequest);

    // 发送事务队列中的请求
    for (Request request : transactionQueue) {
        writeRequest(request);
    }

    // 清空事务队列
    transactionQueue.clear();
}

// 回滚事务时，清空事务队列
public void rollbackTransaction() {
    transactionQueue.clear();
}
```

### 3.复制算法实现GC

DiskDataCenter.java

```java
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException, MqException {
        messageFileManager.deleteMessage(queue, message);
        // 判断是否需要GC
        if (messageFileManager.checkGC(queue.getName())) {
            messageFileManager.gc(queue);
        }
    }
```

MessageFileManager.java

```java
    /**
     * 检查当前是否要针对该队列的消息数据文件进行GC
     * 条件判定：消息总数 > 2000 且 有效消息 / 总消息数 < 0.5
     * @param queueName
     * @return
     */
    public boolean checkGC(String queueName) {
        // 判断是否要GC，是根据总消息数和有效消息数，这两个值都在消息统计文件中
        Stat stat = readStat(queueName);
        return stat.totalCount > 2000 && (double) stat.validCount / (double) stat.totalCount < 0.5;
    }
```

```java
/**
 * 通过这个方法真正执行消息数据文件的垃圾回收操作
 * 使用复制算法来完成，先创建一个新的文件，名字为 queue_data_new.txt
 * 把之前消息数据文件中的有效数据都读出来，写到新的文件中；删除旧的文件，再把新的文件改名回 queue_data.txt
 * 最后要更新消息统计文件
 * @param queue 用作锁对象
 */
public void gc(MSGQueue queue) throws MqException, IOException, ClassNotFoundException {
    synchronized (queue) {
        // gc操作比较耗时，此处统计一下执行的消耗的时间
        long gcBeg = System.currentTimeMillis();

        // 1-创建一个新的文件
        File queueDataNewFile = new File(getQueueDataNewPath(queue.getName()));
        if(queueDataNewFile.exists()) {
            // 正常情况下这个文件是不该存在的，如果存在则说明上一次gc到一半程序意外崩溃了
            throw new MqException("[MessageFileManager] gc 时发现该队列的 queue_data_new 已经存在！queueName = " + queue.getName());
        }
        boolean ok = queueDataNewFile.createNewFile();
        if(!ok) {
            throw new MqException("[MessageFileManager] 创建文件失败！queueDataNewFile = " + queueDataNewFile.getAbsolutePath());
        }

        // 2-从旧的文件中读取出所有的有效消息对象
        LinkedList<Message> messages = loadAllMessageFromQueue(queue.getName());

        // 3-把有效消息写入到新的文件中
        try (OutputStream outputStream = new FileOutputStream(queueDataNewFile)) {
            try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                for(Message message : messages) {
                    byte[] buffer = BinaryTool.toBytes(message);
                    // 先写 4 个字节消息的长度
                    dataOutputStream.writeInt(buffer.length);
                    dataOutputStream.write(buffer);
                }
            }
        }

        // 4-删除旧的数据文件，并把新的数据文件重新命名
        File queueDataOldFile = new File(getQueueDataPath(queue.getName()));
        ok = queueDataOldFile.delete();
        if(!ok) {
            throw new MqException("[MessageFileManager] 删除旧的数据文件失败！queueDataOldFile = " + queueDataOldFile.getAbsolutePath());
        }

        // 把 queue_data_new.txt 重命名为 queue_data.txt
        ok = queueDataNewFile.renameTo(queueDataOldFile);
        if(!ok) {
            throw new MqException("[MessageFileManager] 文件重命名失败！queueDataNewFile = "
                    + queueDataNewFile.getAbsolutePath() + ", queueDataOldFile = " + queueDataOldFile.getAbsolutePath());
        }

        // 5-更新消息统计文件
        Stat stat = readStat(queue.getName());
        stat.totalCount = messages.size();
        stat.validCount = messages.size();
        writeStat(queue.getName(), stat);

        long gcEnd = System.currentTimeMillis();
        System.out.println("[MessageFileManager] gc 执行完毕！queueName = " + queue.getName() +
                ", time = " + (gcEnd-gcBeg) + "ms");
    }
}
```

### 4.从硬盘中恢复数据到内存

MessageFileManager.java

```java
/**
 * 使用这个方法，从文件中读取所有的消息内容，加载到内存中（一个链表里）。
 * 在程序启动的时候进行调用。
 * 不用传入queue对象，因为不需要对queue进行加锁操作。该方法只在程序启动的时候调用，不涉及多线程，因此不用加锁
 * @param queueName
 * @return 返回一个Linkedlist，主要目的是为了方便后序进行的头删操作
 */
public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, MqException, ClassNotFoundException {
    LinkedList<Message> messages = new LinkedList<>();
    try(InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))) {
        try(DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            // 记录当前文件光标
            long currentOffset = 0;
            // 一个文件中包含了很多消息，此处要循环读取
            while(true) {
                // 1-读取当前消息的长度，可能会读到文件末尾（EOF），readInt()读到文件末尾会抛EOFException异常，这一点和很多流对象不一样
                int messageSize = dataInputStream.readInt();
                // 2-按照这个长度，读取消息内容
                byte[] buffer = new byte[messageSize];
                int actualSize = dataInputStream.read(buffer);
                if(messageSize != actualSize) {
                    // 如果不匹配，说明文件有问题，格式错乱了
                    throw new MqException("[MessageFileManager] 文件格式错误！queueName = " + queueName);
                }
                // 3-把读到的二进制数据反序列化为 Message 对象
                Message message = (Message) BinaryTool.fromBytes(buffer);
                // 4-判定一下这个消息对象是否有效
                if(message.getIsValid() != 0x1) {
                    // 无效数据，更新offset，并跳过
                    currentOffset += (4 + messageSize);
                    continue;
                }
                // 5-有效数据，把message对象加入链表中。加入前填写 offsetBeg 和 offsetEnd
                // 进行计算 offset 的时候需要知道当前文件光标的位置。DataInputStream不方便计算，因此要手动计算
                message.setOffsetBeg(currentOffset + 4);
                message.setOffsetEnd(currentOffset + 4 + messageSize);
                currentOffset += (4 + messageSize);
                messages.add(message);
            }
        } catch (EOFException e) {
            // 并非处理异常，而是处理正常的业务逻辑，文件读到末尾readInt会抛出异常
            // catch 中也不需要做什么特殊的事情
            System.out.println("[MessageFileManager] 恢复 Message 数据完成！");
        }
    }
    return messages;
}
```



### 5.实现Topic交换机转发规则bindingKey与routingKey验证算法

```java
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
```

### 6.自定义请求与响应结构

```java
public class Response {
    private int type;
    private int length;
    private byte[] payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
```