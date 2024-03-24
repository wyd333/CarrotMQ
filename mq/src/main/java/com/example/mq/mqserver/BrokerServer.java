package com.example.mq.mqserver;

import com.example.mq.common.*;
import com.example.mq.mqserver.core.BasicProperties;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * Description: 这个 BrokerServer 是消息队列的本体服务器
 * 本质上就是一个 TCP 的服务器
 * User: 12569
 * Date: 2024-03-24
 * Time: 18:39
 */
public class BrokerServer {
    private ServerSocket serverSocket = null;

    // 当前考虑一个 BrokerServer 上只有一个 虚拟主机
    private VirtualHost virtualHost = new VirtualHost("default");
    // 使用这个 哈希表 表示当前的所有会话(也就是说有哪些客户端正在和咱们的服务器进行通信)
    // 此处的 key 是 channelId, value 为对应的 Socket 对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<String, Socket>();
    // 引入一个线程池, 来处理多个客户端的请求.
    private ExecutorService executorService = null;
    // 引入一个 boolean 变量控制服务器是否继续运行
    private volatile boolean runnable = true;
    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void start() throws IOException {
        System.out.println("[BrokerServer] 启动!");
        executorService = Executors.newCachedThreadPool();
        try {
            while (runnable) {
                Socket clientSocket = serverSocket.accept();
                // 把处理连接的逻辑丢给这个线程池
                executorService.submit(() -> {
                    processConnection(clientSocket);
                });
            }
        } catch (SocketException e) {
            System.out.println("[BrokerServer] 服务器停止运行!");
            // e.printStackTrace();
        }
    }

    /**
     *  一般来说停止服务器直接 kill 掉对应进程就行了
     *  此处为了后续的单元测试, 还是搞一个单独的停止方法
     * @throws IOException
     */
    public void stop() throws IOException {
        runnable = false;
        // 把线程池中的任务都放弃了, 让线程都销毁
        executorService.shutdownNow();
        serverSocket.close();
    }

    /**
     * 通过这个方法来处理一个客户端的连接
     * 在这一个连接中可能会涉及到多个请求和响应
     * @param clientSocket
     */
    private void processConnection(Socket clientSocket) {
        try (InputStream inputStream = clientSocket.getInputStream();
             OutputStream outputStream = clientSocket.getOutputStream()) {
            // 这里需要按照特定格式来读取并解析. 此时就需要用到 DataInputStream 和 DataOutputStream
            try (DataInputStream dataInputStream = new DataInputStream(inputStream);
                 DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                while (true) {
                    // 1. 读取请求并解析
                    Request request = readRequest(dataInputStream);
                    // 2. 根据请求计算响应
                    Response response = process(request, clientSocket);
                    // 3. 把响应写回给客户端
                    writeResponse(dataOutputStream, response);
                }
            }
        } catch (EOFException | SocketException e) {
            // 对于这个代码, DataInputStream 如果读到 EOF , 就会抛出一个 EOFException 异常
            // 需要借助这个异常来结束循环
            System.out.println("[BrokerServer] connection 关闭! 客户端的地址: " + clientSocket.getInetAddress().toString()
                    + ":" + clientSocket.getPort());
        } catch (IOException | ClassNotFoundException | MqException e) {
            System.out.println("[BrokerServer] connection 出现异常!");
            e.printStackTrace();
        } finally {
            try {
                // 当连接处理完了, 就需要记得关闭 socket
                clientSocket.close();
                // 一个 TCP 连接中, 可能包含多个 channel. 需要把当前这个 socket 对应的所有 channel 也顺便清理掉
                clearClosedSession(clientSocket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void clearClosedSession(Socket clientSocket) {
        // 这里要做的事情主要就是遍历上述 sessions hash 表, 把该被关闭的 socket 对应的键值对统统删掉
        List<String> toDeleteChannelId = new ArrayList<>();
        for (Map.Entry<String, Socket> entry : sessions.entrySet()) {
            if (entry.getValue() == clientSocket) {
                // 不能在这里直接删除, 会破坏容器结构, 可能造成迭代器失效
                // 这属于使用集合类的一个大忌!!! 一边遍历, 一边删除!!!
                // sessions.remove(entry.getKey());
                toDeleteChannelId.add(entry.getKey());
            }
        }

        // 遍历完了统一删除
        for (String channelId : toDeleteChannelId) {
            sessions.remove(channelId);
        }
        System.out.println("[BrokerServer] 清理 session 完成! 被清理的 channelId=" + toDeleteChannelId);
    }

    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        // 这个刷新缓冲区也是重要的操作!!
        dataOutputStream.flush();
    }

    /**
     * 处理一次请求并返回一个响应
     * @param request
     * @param clientSocket
     * @return
     */
    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException, MqException {
        // 1. 把 request 中的 payload 做一个初步的解析.
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        System.out.println("[Request] rid=" + basicArguments.getRid() + ", channelId=" + basicArguments.getChannelId()
                + ", type=" + request.getType() + ", length=" + request.getLength());
        // 2. 根据 type 的值, 来进一步区分接下来这次请求要干啥.
        boolean ok = true;
        if (request.getType() == 0x1) {
            // 创建 channel
            sessions.put(basicArguments.getChannelId(), clientSocket);
            System.out.println("[BrokerServer] 创建 channel 完成! channelId=" + basicArguments.getChannelId());
        } else if (request.getType() == 0x2) {
            // 销毁 channel
            sessions.remove(basicArguments.getChannelId());
            System.out.println("[BrokerServer] 销毁 channel 完成! channelId=" + basicArguments.getChannelId());
        } else if (request.getType() == 0x3) {
            // 创建交换机. 此时 payload 就是 ExchangeDeclareArguments 对象了.
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            ok = virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeType(),
                    arguments.isDurable(), arguments.isAutoDelete(), arguments.getArguments());
        } else if (request.getType() == 0x4) {
            ExchangeDeleteArguments arguments = (ExchangeDeleteArguments) basicArguments;
            ok = virtualHost.exchangeDelete(arguments.getExchangeName());
        } else if (request.getType() == 0x5) {
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            ok = virtualHost.queueDeclare(arguments.getQueueName(), arguments.isDurable(),
                    arguments.isExclusive(), arguments.isAutoDelete(), arguments.getArguments());
        } else if (request.getType() == 0x6) {
            QueueDeleteArguments arguments = (QueueDeleteArguments) basicArguments;
            ok = virtualHost.queueDelete((arguments.getQueueName()));
        } else if (request.getType() == 0x7) {
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            ok = virtualHost.queueBind(arguments.getQueueName(), arguments.getExchangeName(), arguments.getBindingKey());
        } else if (request.getType() == 0x8) {
            QueueUnbindArguments arguments = (QueueUnbindArguments) basicArguments;
            ok = virtualHost.queueUnbind(arguments.getQueueName(), arguments.getExchangeName());
        } else if (request.getType() == 0x9) {
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            ok = virtualHost.basicPublish(arguments.getExchangeName(), arguments.getRoutingKey(),
                    arguments.getBasicProperties(), arguments.getBody());
        } else if (request.getType() == 0xa) {
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            ok = virtualHost.basicConsume(arguments.getConsumerTag(), arguments.getQueueName(), arguments.isAutoAck(),
                    new Consumer() {
                        /**
                         * 这个回调函数要做的工作, 就是把服务器收到的消息可以直接推送回对应的消费者客户端
                         * @param consumerTag
                         * @param basicProperties
                         * @param body
                         * @throws MqException
                         * @throws IOException
                         */
                        @Override
                        public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws IOException, MqException {
                            // 先知道当前这个收到的消息要发给哪个客户端
                            // 此处 consumerTag 其实是 channelId. 根据 channelId 去 sessions 中查询, 就可以得到对应的
                            // socket 对象了, 从而可以往里面发送数据了

                            // 1. 根据 channelId 找到 socket 对象
                            Socket clientSocket = sessions.get(consumerTag);
                            if (clientSocket == null || clientSocket.isClosed()) {
                                throw new MqException("[BrokerServer] 订阅消息的客户端已经关闭!");
                            }

                            // 2. 构造响应数据
                            SubScribeReturns subScribeReturns = new SubScribeReturns();
                            subScribeReturns.setChannelId(consumerTag);
                            subScribeReturns.setRid(""); // 由于这里只有响应, 没有请求, 不需要去对应, rid 暂时不需要
                            subScribeReturns.setOk(true);
                            subScribeReturns.setConsumerTag(consumerTag);
                            subScribeReturns.setBasicProperties(basicProperties);
                            subScribeReturns.setBody(body);
                            byte[] payload = BinaryTool.toBytes(subScribeReturns);
                            Response response = new Response();
                            // 0xc 表示服务器给消费者客户端推送的消息数据.
                            response.setType(0xc);
                            // response 的 payload 就是一个 SubScribeReturns
                            response.setLength(payload.length);
                            response.setPayload(payload);
                            // 3. 把数据写回给客户端.
                            //    注意! 此处的 dataOutputStream 这个对象不能 close !!!
                            //    如果 把 dataOutputStream 关闭, 就会直接把 clientSocket 里的 outputStream 也关了.
                            //    此时就无法继续往 socket 中写入后续数据了.
                            DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
                            writeResponse(dataOutputStream, response);
                        }
                    });
        } else if (request.getType() == 0xb) {
            // 调用 basicAck 确认消息.
            BasicAckArguments arguments = (BasicAckArguments) basicArguments;
            ok = virtualHost.basicAck(arguments.getQueueName(), arguments.getMessageId());
        } else {
            // 当前的 type 是非法的.
            throw new MqException("[BrokerServer] 未知的 type! type=" + request.getType());
        }
        // 3. 构造响应
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setOk(ok);
        byte[] payload = BinaryTool.toBytes(basicReturns);
        Response response = new Response();
        response.setType(request.getType());
        response.setLength(payload.length);
        response.setPayload(payload);
        System.out.println("[Response] rid=" + basicReturns.getRid() + ", channelId=" + basicReturns.getChannelId()
                + ", type=" + response.getType() + ", length=" + response.getLength());
        return response;
    }

    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] payload = new byte[request.getLength()];
        int n = dataInputStream.read(payload);
        if (n != request.getLength()) {
            throw new IOException("读取请求格式出错!");
        }
        request.setPayload(payload);
        return request;
    }
}
