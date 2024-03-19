package com.example.mq;

import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.datacenter.MemoryDataCenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 12569
 * Date: 2024-03-19
 * Time: 8:30
 */

@SpringBootTest
public class MemoryDataCenterTest {
    private MemoryDataCenter memoryDataCenter = null;

    @BeforeEach
    public void setUp() {
        memoryDataCenter = new MemoryDataCenter();  // 内存中操作，要保证测试用例之间的数据隔离
    }

    @AfterEach
    public void tearDown() {
        memoryDataCenter = null;
    }

    // 创建一个测试交换机
    private Exchange createTestExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.DIRECT);
        exchange.setAutoDelete(false);
        exchange.setDurable(true);
        return exchange;
    }

    // 创建一个测试队列
    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(false);
        return queue;
    }

    // 针对交换机进行测试
    @Test
    public void testExchange() {
        // 1. 先构造一个交换机并插入
        Exchange expectedExchange = createTestExchange("testExchange");
        memoryDataCenter.insertExchange(expectedExchange);

        // 2. 查询出这个交换机，比较结果是否一致。此处直接比较这俩引用指向同一个对象
        Exchange actualExchange = memoryDataCenter.getExchange("testExchange");
        Assertions.assertEquals(expectedExchange, actualExchange);

        // 3. 删除这个交换机
        memoryDataCenter.deleteExchange("testExchange");
        // 4. 再查一次，看是否查询不到
        actualExchange = memoryDataCenter.getExchange("testExchange");
        Assertions.assertNull(actualExchange);
    }
}
