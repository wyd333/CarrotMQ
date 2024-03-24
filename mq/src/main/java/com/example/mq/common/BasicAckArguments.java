package com.example.mq.common;

import java.io.Serializable;

public class BasicAckArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String messageId;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}
