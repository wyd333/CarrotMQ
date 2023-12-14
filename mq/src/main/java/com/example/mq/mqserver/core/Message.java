package com.example.mq.mqserver.core;

/**
 * Created with IntelliJ IDEA.
 * Description: 要传递的消息
 * User: 12569
 * Date: 2023-11-21
 * Time: 22:19
 */
public class Message {
    // 这两个属性是message最核心的部分
    private  BasicProperties basicProperties;
    private byte[] body;

    // 辅助用的属性
    // Message后续会存储到文件中，如果持久化的话
    // 一个文件中存储很多的消息
    // 使用下面两个偏移量来表示消息在文件中的具体位置，以找到某个消息
    // 前闭后开：[offsetBeg, offsetEnd)
    private long offsetBeg = 0; // 消息数据的开头距离文件开头的位置偏移/字节
    private long offsetEnd = 0; // 消息数据的结尾距离文件开头的位置偏移/字节
    // 使用这个属性表示该消息在文件中是否是有效消息，针对文件中的消息，如果删除就使用逻辑删除
    // 0x1：有效，0x0：无效    用byte类型方便对字节进行操作，boolean类型在文件中不一定是一个字节git
    private byte isValid = 0x1;
}
