package com.example.mq.mqclient;

import com.example.mq.common.*;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Connection {
    private Socket socket = null;
    // 需要管理多个 channel, 使用一个 hash 表把若干个 channel 组织起来
    private ConcurrentHashMap<String, Channel> channelMap = new ConcurrentHashMap<>();

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    private ExecutorService callbackPool = null;
    private LinkedList<Request> transactionQueue = new LinkedList<>();

    public Connection(String host, int port) throws IOException {
        socket = new Socket(host, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);

        callbackPool = Executors.newFixedThreadPool(4);

        // 创建一个扫描线程, 由这个线程负责不停的从 socket 中读取响应数据
        // 把这个响应数据再交给对应的 channel 负责处理
        Thread t = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    Response response = readResponse();
                    dispatchResponse(response);
                }
            } catch (SocketException e) {
                // 连接正常断开的. 此时这个异常直接忽略
                System.out.println("[Connection] 连接正常断开!");
            } catch (IOException | ClassNotFoundException | MqException e) {
                System.out.println("[Connection] 连接异常断开!");
                e.printStackTrace();
            }
        });
        t.start();
    }

    public void close() {
        // 关闭 Connection 释放上述资源
        try {
            callbackPool.shutdownNow();
            channelMap.clear();
            inputStream.close();
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 使用这个方法来分别处理, 当前的响应是一个针对控制请求的响应, 还是服务器推送的消息
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, MqException {
        if (response.getType() == 0xc) {
            // 服务器推送来的消息数据
            SubScribeReturns subScribeReturns = (SubScribeReturns) BinaryTool.fromBytes(response.getPayload());
            // 根据 channelId 找到对应的 channel 对象
            Channel channel = channelMap.get(subScribeReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 该消息对应的 channel 在客户端中不存在! channelId=" + channel.getChannelId());
            }
            // 执行该 channel 对象内部的回调
            callbackPool.submit(() -> {
                try {
                    channel.getConsumer().handleDelivery(subScribeReturns.getConsumerTag(), subScribeReturns.getBasicProperties(),
                            subScribeReturns.getBody());
                } catch (MqException | IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
            // 当前响应是针对刚才的控制请求的响应
            BasicReturns basicReturns = (BasicReturns) BinaryTool.fromBytes(response.getPayload());
            // 把这个结果放到对应的 channel 的 hash 表中.
            Channel channel = channelMap.get(basicReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 该消息对应的 channel 在客户端中不存在! channelId=" + channel.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }

    // 发送请求
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
        System.out.println("[Connection] 发送请求! type=" + request.getType() + ", length=" + request.getLength());
    }

    // 读取响应
    public Response readResponse() throws IOException {
        Response response = new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload = new byte[response.getLength()];
        int n = dataInputStream.read(payload);
        if (n != response.getLength()) {
            throw new IOException("读取的响应数据不完整!");
        }
        response.setPayload(payload);
        System.out.println("[Connection] 收到响应! type=" + response.getType() + ", length=" + response.getLength());
        return response;
    }

    // 通过这个方法, 在 Connection 中能够创建出一个 Channel
    public Channel createChannel() throws IOException {
        String channelId = "C-" + UUID.randomUUID().toString();
        Channel channel = new Channel(channelId, this);
        // 把这个 channel 对象放到 Connection 管理 channel 的 哈希表 中
        channelMap.put(channelId, channel);
        // 同时也需要把 "创建 channel" 的这个消息也告诉服务器
        boolean ok = channel.createChannel();
        if (!ok) {
            // 服务器这里创建失败了!! 整个这次创建 channel 操作不顺利!!
            // 把刚才已经加入 hash 表的键值对, 再删了
            channelMap.remove(channelId);
            return null;
        }
        return channel;
    }

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
}
