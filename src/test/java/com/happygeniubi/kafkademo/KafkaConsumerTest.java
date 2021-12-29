package com.happygeniubi.kafkademo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaConsumerTest {

    private static final String TOPIC_NAME = "happygeniubi-topic";
    private static final String ICC_SEND_TOPIC = "icc-send-topic";

    public static Properties getProperties() {
        Properties props = new Properties();
        // broker地址
        props.put("bootstrap.servers", "127.0.0.1:9092");

        // 消费者分组Id,分组内的消费者只能消费该消息一次,不同分组内的消费者可以重复消息该消息
        props.put("group.id", "happygeniubi-g1");

        // 默认是latest, 如果需要从头消费partition消息, 需要改为earliest且消费组名变更,才生效
        props.put("auto.offset.reset", "earliest");

        // 开启自动提交offset
//        props.put("enable.auto.commit", "true");
        props.put("enable.auto.commit", "false");

        // 自动提交offset延迟时间
//        props.put("auto.commit.interval.ms", "1000");

        // 反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Test
    public void simpleCousumerTest() {
        Properties properties = getProperties();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // 订阅主题
        kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            System.err.println("开始拉取...");
            // 拉取订阅内容(阻塞超时时间,100毫秒)
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.err.printf("topic=%s, offset=%d, key=%s, value=%s %n",
                        record.topic(), record.offset(), record.key(), record.value());
            }
            if(!records.isEmpty()) {
                // 同步阻塞提交offset
//            kafkaConsumer.commitSync();
                // 异步提交offset
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if (e == null) {
                            System.err.println("手工提交offset成功:" + map.toString());
                        } else {
                            System.err.println("手工提交失败");
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }
}
