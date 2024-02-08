package com.example.mq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class MqApplication {
    public static ConfigurableApplicationContext context;
    public static void main(String[] args) {

        context = SpringApplication.run(MqApplication.class, args);
    }

}
