package com.example.mq.mqserver.core;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 12569
 * Date: 2023-11-22
 * Time: 23:33
 */
public enum ExchangeType {
    DIRECT(0),
    FANOUT(1),
    TOPIC(2);

    private final int type;

    private ExchangeType(int type){
        this.type = type;
    }

    public int getType(){
        return type;
    }
}
