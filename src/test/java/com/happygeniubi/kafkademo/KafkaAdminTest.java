package com.happygeniubi.kafkademo;

import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminTest {

    private static final String TOPIC_NAME = "happygeniubi-topic";
    private static final String TOPIC_NAME_V2 = "happygeniubi-topic-v2";

    /**
     * 获取Kafka客户端
     * @return
     */
    public static AdminClient initAdminClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * 创建topic
     */
    @Test
    public void createTopic() {
        AdminClient adminClient = initAdminClient();
        // 两个分区一个副本
        NewTopic newTopic = new NewTopic(TOPIC_NAME_V2, 5 , (short) 1);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        try {
            // future等待创建,成功不会有任何报错,如果创建失败和超时会报错
            createTopicsResult.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Topic创建成功!");
    }

    /**
     * 列举topic列表
     * @throws Exception
     */
    @Test
    public void listTopicList() throws Exception {
        AdminClient adminClient = initAdminClient();

        // 是否查看内部的topic,默认为false
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);

        ListTopicsResult listTopicsResult = adminClient.listTopics(options);

        Set<String> topics = listTopicsResult.names().get();
        for (String topic : topics) {
            System.err.println(topic);
        }
    }

    /**
     * 删除topic
     * @throws Exception
     */
    @Test
    public void delTopicTest() throws Exception {
        AdminClient adminClient = initAdminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
        System.out.println("删除成功");
    }

    /**
     * 获取指定topic的详细信息
     * @throws Exception
     */
    @Test
    public void getTopicInfo() throws Exception {
        AdminClient adminClient = initAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.forEach((entry)-> System.out.println("name:" + entry.getKey() + " , desc: " + entry.getValue()));
    }

    /**
     * 增加分区数量
     * 如果主题中的消息包含有Key时（即Key不为null），根据Key来计算分区的行为就会有所影响消息顺序性
     * Kafka中的分区只能增加不能减少，减少的话数据不知道怎么处理
     * @throws Exception
     */
    @Test
    public void incrPartitionsTest() throws Exception{
        Map<String, NewPartitions> infoMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        AdminClient adminClient = initAdminClient();
        infoMap.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(infoMap);
        createPartitionsResult.all().get();
    }
}
