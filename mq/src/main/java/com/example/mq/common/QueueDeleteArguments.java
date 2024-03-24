package com.example.mq.common;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 12569
 * Date: 2024-03-24
 * Time: 17:47
 */
public class QueueDeleteArguments extends BasicArguments implements Serializable {
    private String queueName;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
