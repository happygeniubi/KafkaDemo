package com.happygeniubi.kafkademo.controller;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.SuccessCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    private static final String TOPIC_NAME = "Happygeniubi666";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/api/v1/{num}")
    public void sendMessage(@PathVariable("num") String num) {
        kafkaTemplate.send(TOPIC_NAME, "这是一个消息,num=" + num).addCallback(success -> {
            String topic = success.getRecordMetadata().topic();
            int partition = success.getRecordMetadata().partition();
            long offset = success.getRecordMetadata().offset();
            System.out.println("发送成功:topic="+topic+", 分区编号partition="+partition+", 偏移量offset="+offset);
        }, failure -> {
            System.out.println("发送失败:" + failure.getMessage());
        });
    }

    /**
     * 注解方式的事务
     */
    @GetMapping("/api/v1/tran")
    @Transactional(rollbackFor = RuntimeException.class)
    public void sendMessageTran() {
        for (int i=0; i<2; i++) {
            if (i==1) {
                throw new RuntimeException();
            }
            kafkaTemplate.send(TOPIC_NAME, "这是一个事务消息,i=" + i).addCallback(success -> {
                String topic = success.getRecordMetadata().topic();
                int partition = success.getRecordMetadata().partition();
                long offset = success.getRecordMetadata().offset();
                System.out.println("发送成功:topic=" + topic + ", 分区编号partition=" + partition + ", 偏移量offset=" + offset);
            }, failure -> {
                System.out.println("发送失败:" + failure.getMessage());
            });
        }
    }

    /**
     * 注解方式的事务
     */
    @GetMapping("/api/v1/tran2")
    public void sendMessageTran2() {
        for (int i=0; i<2; i++) {
            if (i==1) {
                throw new RuntimeException();
            }
            kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback<String, Object, Object>() {
                @Override
                public Object doInOperations(KafkaOperations<String, Object> kafkaOperations) {
                    for (int i=0; i<2; i++) {
                        if (i==1) {
                            throw new RuntimeException();
                        }
                        kafkaTemplate.send(TOPIC_NAME, "这是一个事务消息,i=" + i).addCallback(success -> {
                            String topic = success.getRecordMetadata().topic();
                            int partition = success.getRecordMetadata().partition();
                            long offset = success.getRecordMetadata().offset();
                            System.out.println("发送成功:topic=" + topic + ", 分区编号partition=" + partition + ", 偏移量offset=" + offset);
                        }, failure -> {
                            System.out.println("发送失败:" + failure.getMessage());
                        });
                    }
                    return true;
                }
            });
        }
    }
}
