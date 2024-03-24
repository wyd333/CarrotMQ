package com.example.mq.common;

import java.io.Serializable;

public class QueueUnbindArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String exchangeName;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }
}
