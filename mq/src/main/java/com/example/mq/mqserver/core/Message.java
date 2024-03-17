package com.example.mq.mqserver.core;

import com.sun.org.apache.bcel.internal.generic.NEW;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * Description: 要传递的消息
 * User: 12569
 * Date: 2023-11-21
 * Time: 22:19
 */

/**
 * Message对象需要能够在网络中传输，并且也需要写入文件中
 * 此处需要对Message进行序列化和反序列化
 * 使用标准库自带的序列虎啊/反序列化方式
 * （不使用json，Message中存储的是二进制，而json本质上是文本格式）
 * 实现Serializable接口，但offsetBeg和offsetEnd不用被序列化保存到文件中
 * （消息被写入文件中，位置就固定了，不需要单独存储；这两个属性的目的是让内存中的Message对象能
 * 快速找到对应硬盘上Message的位置）
 */
public class Message implements Serializable {
    // 这两个属性是message最核心的部分
    private BasicProperties basicProperties = new BasicProperties();    // 消息属性
    private byte[] body;    // 消息正文


    // 辅助用的属性
    // Message后续会存储到文件中，如果持久化的话
    // 一个文件中存储很多的消息
    // 使用下面两个偏移量来表示消息在文件中的具体位置，以找到某个消息
    // 前闭后开：[offsetBeg, offsetEnd)
    // transient关键字：不被标准库序列化
    private transient long offsetBeg = 0; // 消息数据的开头距离文件开头的位置偏移/字节
    private transient long offsetEnd = 0; // 消息数据的结尾距离文件开头的位置偏移/字节
    // 使用这个属性表示该消息在文件中是否是有效消息，针对文件中的消息，如果删除就使用逻辑删除
    // 0x1：有效，0x0：无效    用byte类型方便对字节进行操作，boolean类型在文件中不一定是一个字节git
    private byte isValid = 0x1;

    // 创建一个工厂方法，让工厂方法封装创建 Message 对象的过程
    // 此方法创建的 Message 对象会生成一个唯一的 MessageId
    public static Message createMessageWithId(String routingKey,
                                              BasicProperties basicProperties,
                                              byte[] body){
        Message message = new Message();
        if(basicProperties != null) {
            message.setBasicProperties(basicProperties);
        }

        message.setMessageId("M-" + UUID.randomUUID());
        // 万一routingKey和basicProperties中的routingKey冲突，以外面的为主
        message.setRoutingKey(routingKey);
        message.body = body;
        // 此处把Message的核心 body 和 basicProperties先设置出来
        // offsetBeg、offsetEnd、isValid 在消息持久化的时候才会用到，在把消息写入文件之前再进行设置
        // 此处只是在内存中创建一个Message对象
        return message;
    }


    public String getMessageId(){
        return basicProperties.getMessageId();
    }

    public void setMessageId(String messageId){
        basicProperties.setMessageId(messageId);
    }

    public String getRoutingKey(){
        return basicProperties.getRoutingKey();
    }
    public void setRoutingKey(String routingKey){
        basicProperties.setRoutingKey(routingKey);
    }
    public int getDeliverMode(){
        return basicProperties.getDeliverMode();
    }

    public void setDeliverMode(int mode){
        basicProperties.setDeliverMode(mode);
    }
    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getOffsetBeg() {
        return offsetBeg;
    }

    public void setOffsetBeg(long offsetBeg) {
        this.offsetBeg = offsetBeg;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    public void setOffsetEnd(long offsetEnd) {
        this.offsetEnd = offsetEnd;
    }

    public byte getIsValid() {
        return isValid;
    }

    public void setIsValid(byte isValid) {
        this.isValid = isValid;
    }

    @Override
    public String toString() {
        return "Message{" +
                "basicProperties=" + basicProperties +
                ", body=" + Arrays.toString(body) +
                ", offsetBeg=" + offsetBeg +
                ", offsetEnd=" + offsetEnd +
                ", isValid=" + isValid +
                '}';
    }
}
