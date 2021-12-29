package com.happygeniubi.kafkademo.mq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class MQListener {

    private static final String TOPIC_NAME = "Happygeniubi666";

    @KafkaListener(topics = TOPIC_NAME, groupId = "happygeniubi-gp1")
    public void onMessage(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println("消费消息:" + record.topic()+ "|" + record.partition() + "|" + record.value());
        // 确认消费
        ack.acknowledge();
    }
}
