package com.meng.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Description 消费者类
 * 消费者不是线程安全的，一个线程只能对应一个消费者
 * @Date 2019/3/29 2:30
 * @Author LSM
 **/
public class MyConsumer {

    /** kafka自己封装的消费者 **/
    private static KafkaConsumer<String ,String> consumer;
    /** 新建配置 **/
    private static Properties properties;

    /** 生产者发送序列化数据，消费者消费反序列化数据 **/
    static {
        properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        /** 消费者组 **/
        properties.put("group.id","KafkaLearn");
    }

    /**
     * 自动提交位移
     */
    private static void generalConsumerMessageAutoComit(){

        /** 默认每隔5秒提交一次位移 **/
        properties.put("enable.auto.commit",true);
        consumer = new KafkaConsumer<>(properties);
        /** 订阅topic，消费该topic的消息 **/
        consumer.subscribe(Collections.singleton("kafka-study"));

        try{
            while (true){
                /** 用来退出死循环 **/
                boolean flag = true;
                /** 拉取数据为多条 **/
                ConsumerRecords<String,String> records = consumer.poll(100);
                /** 遍历拉取的数据进行输出 **/
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format(
                            "topic = %s, partition = %s, key = %s, value = %s",
                            record.topic(), record.partition(), record.key(), record.value()
                    ));
                    /** 如果value的值为done时遍历完成 **/
                    if (record.value().equals("done")){
                        flag = false;
                    }
                }
                /** 退出死循环 **/
                if(!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }

    private static void generalConsumeMessageSyncCommit() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("kafka-study"));

        while (true) {
            boolean flag = true;

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format(
                        "topic = %s, partition = %s, key = %s, value = %s",
                        record.topic(), record.partition(), record.key(), record.value()
                ));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            try {
                consumer.commitSync();
            } catch (CommitFailedException ex) {
                System.out.println("commit failed error:" + ex.getMessage());
            }

            if (!flag) {
                break;
            }
        }
    }

    private static void generalConsumeMessageAsyncCommit() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("kafka-study"));

        while (true) {
            boolean flag = true;

            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format(
                        "topic = %s, partition = %s, key = %s, value = %s",
                        record.topic(), record.partition(), record.key(), record.value()
                ));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            // commit A, offset 2000
            // commit B, offset 3000
            consumer.commitAsync();

            if (!flag) {
                break;
            }
        }
    }

    private static void generalConsumeMessageAsyncCommitWithCallback() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("kafka-study"));

        while (true) {
            boolean flag = true;

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format(
                        "topic = %s, partition = %s, key = %s, value = %s",
                        record.topic(), record.partition(), record.key(), record.value()
                ));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            consumer.commitAsync((map, e) -> {
                if (e != null) {
                    System.out.println("commit failed for offsets: " + e.getMessage());
                }
            });

            if (!flag) {
                break;
            }
        }
    }

    @SuppressWarnings("all")
    private static void mixSyncAndAsyncCommit() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("kafka-study"));

        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format(
                            "topic = %s, partition = %s, key = %s, value = %s",
                            record.topic(), record.partition(), record.key(), record.value()
                    ));
                }

                consumer.commitAsync();
            }
        } catch (Exception ex) {
            System.out.println("commit async error: " + ex.getMessage());
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        generalConsumerMessageAutoComit();
    }

}
