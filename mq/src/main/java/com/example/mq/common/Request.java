package com.example.mq.common;

/**
 * Created with IntelliJ IDEA.
 * Description: 表示一个网络通信中的请求对象, 按照自定义协议的格式来展开
 * User: 12569
 * Date: 2024-03-24
 * Time: 16:54
 */
public class Request {
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
