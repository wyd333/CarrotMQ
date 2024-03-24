package com.example.mq.mqclient;

import java.io.IOException;

public class ConnectionFactory {
    // broker server 的 ip 地址
    private String host;
    // broker server 的端口号
    private int port;

    // 访问 broker server 的哪个虚拟主机
    // 下列几个属性暂时先不实现
//    private String virtualHostName;
//    private String username;
//    private String password;

    public Connection newConnection() throws IOException {
        Connection connection = new Connection(host, port);
        return connection;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
