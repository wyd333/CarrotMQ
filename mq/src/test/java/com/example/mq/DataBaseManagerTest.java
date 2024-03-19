package com.example.mq;

import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.datacenter.DataBaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description: DataBaseManager的单元测试类
 * User: 12569
 * Date: 2024-02-08
 * Time: 22:28
 */

@SpringBootTest
public class DataBaseManagerTest {
    private DataBaseManager dataBaseManager = new DataBaseManager();

    // 准备工作，每个测试用例执行前调用
    @BeforeEach
    public void setUp(){
        // 在init中通过context对象得到metaMapper实例，所以要先构建出context对象
        MqApplication.context = SpringApplication.run(MqApplication.class);
        // 有了context对象才能进行后续操作
        dataBaseManager.init();
    }

    // 收尾工作，每个用例执行后调用
    @AfterEach
    public void tearDown(){
        // 把数据库清空，把数据库清空（删除meta.db文件）
        // 不能直接删除，必须先关闭context对象，否则会出错：context对象持有了MetaMapper实例，而MetaMapper实例又打开meta.db文件
        // 即当meta.db被别人打开时删除该文件是不会成功的，是Windows系统的限制，Linux没有问题
        // 另一方面获取context操作会占用8080端口，此处close也是释放8080端口
        MqApplication.context.close();
        dataBaseManager.deleteDB();
    }


    @Test
    public void testInitTable(){
        // init方法已经在上面setUp方法种调用过了，直接在测试用例代码中检查当前数据库的状态即可
        // 直接从数据库中查询，看数据是否符合预期
        // 查交交换机表，里面应该只有一个数据（匿名exchange）
        // 查队列表，没有数据
        // 查绑定表，没有数据
        List<Exchange> exchangeList = dataBaseManager.selectAllExchanges();
        List<MSGQueue> queueList = dataBaseManager.selectAllQueues();
        List<Binding> bindingList = dataBaseManager.selectAllBindings();

        // 断言结果是否相等
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
        Assertions.assertEquals(ExchangeType.DIRECT, exchangeList.get(0).getType());
        Assertions.assertEquals(0,queueList.size());
        Assertions.assertEquals(0,bindingList.size());
    }

    private Exchange createTestExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.FANOUT);
        exchange.setAutoDelete(false);
        exchange.setDurable(true);
        exchange.setArguments("aaa", 1);
        exchange.setArguments("bbb", 2);
        return exchange;
    }

    @Test
    public void testInsertExchange() {
        // 构造Exchange对象插入数据库中，再查询出来看结果是否符合预期
        Exchange exchange = createTestExchange("testExchange");
        dataBaseManager.insertExchange(exchange);
        // 插入完毕后查询结果
        List<Exchange> exchangeList = dataBaseManager.selectAllExchanges();
        Assertions.assertEquals(2, exchangeList.size());
        Exchange newExchange = exchangeList.get(1);
        Assertions.assertEquals("testExchange", newExchange.getName());
        Assertions.assertEquals(ExchangeType.FANOUT, newExchange.getType());
        Assertions.assertEquals(false, newExchange.isAutoDelete());
        Assertions.assertEquals(true, newExchange.isDurable());
        Assertions.assertEquals(1, newExchange.getArguments("aaa"));
        Assertions.assertEquals(2, newExchange.getArguments("bbb"));
    }


    @Test
    public void testDeleteExchange(){
        // 先构造一个交换机插入数据库，然后按照名字删除即可
        Exchange exchange = createTestExchange("testExchange");
        dataBaseManager.insertExchange(exchange);
        List<Exchange> exchangeList = dataBaseManager.selectAllExchanges();
        Assertions.assertEquals(2, exchangeList.size());
        Assertions.assertEquals("testExchange", exchangeList.get(1).getName());

        // 进行删除操作
        dataBaseManager.deleteExchange("testExchange");
        // 再次查询
        exchangeList = dataBaseManager.selectAllExchanges();
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
    }

    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        queue.setArguments("aaa", 1);
        queue.setArguments("bbb", 2);
        return queue;
    }

    @Test
    public void testInsertQueue() {
        MSGQueue queue = createTestQueue("testQueue");
        dataBaseManager.insertQueue(queue);
        List<MSGQueue> queueList = dataBaseManager.selectAllQueues();

        Assertions.assertEquals(1, queueList.size());
        Assertions.assertEquals("testQueue", queueList.get(0).getName());
        Assertions.assertEquals(true, queueList.get(0).isDurable());
        Assertions.assertEquals(false, queueList.get(0).isAutoDelete());
        Assertions.assertEquals(false, queueList.get(0).isExclusive());
        Assertions.assertEquals(1, queueList.get(0).getArguments("aaa"));
        Assertions.assertEquals(2, queueList.get(0).getArguments("bbb"));
    }

    @Test
    public void testDeleteQueue() {
        MSGQueue queue = createTestQueue("testQueue");
        dataBaseManager.insertQueue(queue);
        List<MSGQueue> queueList = dataBaseManager.selectAllQueues();
        Assertions.assertEquals(1, queueList.size());
        // 进行删除
        dataBaseManager.deleteQueue("testQueue");
        queueList = dataBaseManager.selectAllQueues();
        Assertions.assertEquals(0, queueList.size());
    }

    private Binding createTestBinding(String exchangeName, String queueName) {
        Binding binding = new Binding();
        binding.setExchangeName(exchangeName);
        binding.setQueueName(queueName);
        binding.setBindingKey("testBindingKey");
        return binding;
    }
    @Test
    public void testInsertBinding() {
        Binding binding = createTestBinding("testExchange", "testQueue");
        dataBaseManager.insertBinding(binding);

        List<Binding> bindingList = dataBaseManager.selectAllBindings();
        Assertions.assertEquals(1, bindingList.size());
        Assertions.assertEquals("testQueue", bindingList.get(0).getQueueName());
        Assertions.assertEquals("testExchange", bindingList.get(0).getExchangeName());
        Assertions.assertEquals("testBindingKey", bindingList.get(0).getBindingKey());
    }

    @Test
    public void testDeleteBinding() {
        Binding binding = createTestBinding("testExchange", "testQueue");
        dataBaseManager.insertBinding(binding);
        List<Binding> bindingList = dataBaseManager.selectAllBindings();
        Assertions.assertEquals(1, bindingList.size());

        Binding toDeleteBinding = createTestBinding("testExchange", "testQueue");
        dataBaseManager.deleteBinding(toDeleteBinding);

        bindingList = dataBaseManager.selectAllBindings();
        Assertions.assertEquals(0, bindingList.size());
    }
}
