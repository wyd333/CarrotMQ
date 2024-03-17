package com.example.mq;

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;
import com.example.mq.mqserver.datacenter.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 12569
 * Date: 2024-03-17
 * Time: 17:04
 */

@SpringBootTest
class MessageFileManagerTest {
    private MessageFileManager messageFileManager = new MessageFileManager();
    private static final String queueName1 = "testQueue1";
    private static final String queueName2 = "testQueue2";

    /**
     * 每个用例执行之前的准备工作
     */
    @BeforeEach
    public void setUp() throws IOException {
        // 准备阶段，创建出两个队列以备用
        messageFileManager.createQueueFiles(queueName1);
        messageFileManager.createQueueFiles(queueName2);
    }

    /**
     * 每个用例执行完毕之后的收尾工作
     */
    @AfterEach
    public void tearDown() throws IOException {
        // 收尾阶段，把创建出的队列删除
        messageFileManager.destroyQueueFiles(queueName1);
        messageFileManager.destroyQueueFiles(queueName2);
    }

    @Test
    void testCreateFiles() {
        // 创建队列已在 setUp 中执行过了，此处主要验证文件是否存在
        File queueDataFile1 = new File("./data/" + queueName1 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile1.isFile());     // 要保证既存在又是一个普通文件
        File queueStatFile1 = new File("./data/" + queueName1 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStatFile1.isFile());     // 要保证既存在又是一个普通文件

        File queueDataFile2 = new File("./data/" + queueName2 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile2.isFile());     // 要保证既存在又是一个普通文件
        File queueStatFile2 = new File("./data/" + queueName2 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStatFile2.isFile());     // 要保证既存在又是一个普通文件
    }

    @Test
    public void testReadWriteStat(){
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.totalCount = 100;
        stat.validCount = 50;

        // 此处需要用反射的方式来调用 writeStat 和 readStat
        // 使用Spring封装好的反射工具类
        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", queueName1, stat);

        // 写入完毕后再调用一下读取，验证读取的结果和写入的数据是一致的
        MessageFileManager.Stat newStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        Assertions.assertEquals(100, newStat.totalCount);
        Assertions.assertEquals(50, newStat.validCount);
    }

    /**
     * 构造队列对象
     * @param queueName
     * @return
     */
    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        return queue;
    }

    /**
     * 构造消息对象
     * @param content
     * @return
     */
    private Message createTestMessage(String content) {
        Message message = Message.createMessageWithId("testRoutingKey", null, content.getBytes());
        return message;
    }

    @Test
    public void testSendMessage() throws IOException, MqException, ClassNotFoundException {
        // 构造出消息，并且构造出队列
        Message message = createTestMessage("testMessage");
        // 此处创建的 queue 对象的 name 不能随便写，只能用 queueName1 和 queueName2
        // 需要保证这个队列对象对应的目录和文件啥的都存在才行.
        MSGQueue queue = createTestQueue(queueName1);

        // 调用发送消息方法
        messageFileManager.sendMessage(queue, message);

        // 检查 stat 文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        Assertions.assertEquals(1, stat.totalCount);
        Assertions.assertEquals(1, stat.validCount);

        // 检查 data 文件
        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(1, messages.size());
        Message curMessage = messages.get(0);
        Assertions.assertEquals(message.getMessageId(), curMessage.getMessageId());
        Assertions.assertEquals(message.getRoutingKey(), curMessage.getRoutingKey());
        Assertions.assertEquals(message.getDeliverMode(), curMessage.getDeliverMode());
        // 比较两个字节数组的内容是否相同不能直接使用 assertEquals
        Assertions.assertArrayEquals(message.getBody(), curMessage.getBody());

        System.out.println("message: " + curMessage);


    }
}