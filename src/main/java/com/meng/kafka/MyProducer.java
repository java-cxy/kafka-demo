package com.meng.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Description生产者类
 * @Date 2019/3/28 14:38
 * @Author LSM
 **/
public class MyProducer {

    /** kafka自己封装的生产者 **/
    private static KafkaProducer<String,String> producer;

    /** 配置生产者初始化时的属性 **/
    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        /** 配置使用自定义分配器 **/
        properties.put("partitioner.class",
                "com.meng.kafka.CustomPartitioner");
        producer = new KafkaProducer<>(properties);
    }

    /****
     * 只发送不关心结果
     */
    private static void sendMassageForgetResult(){
        ProducerRecord<String,String> record = new ProducerRecord<>(
                "kafka-study","name","ForgetResult"
        );
        producer.send(record);
        producer.close();
    }

    /**
     * 同步发送并接收返回信息
     */
    private static void sendMassageSync() throws Exception{
        ProducerRecord<String,String> record = new ProducerRecord<>(
                "kafka-study","name","sync"
        );
        RecordMetadata result = producer.send(record).get();
        System.out.println("topic:"+result.topic());
        System.out.println("分区信息："+result.partition());
        System.out.println("偏移量："+result.offset());
        producer.close();

    }

    /**
     * 异步发送消息，返回消息到回调类中打印
     */
    private static void sendMassageCallBack(){
        ProducerRecord<String,String> record = new ProducerRecord<>(
                "kafka-study","name","CallBack"
        );
        producer.send(record,new MyProducerCallBack());
        producer.close();
    }

    /**
     * 异步发送消息回调类
     */
    private static class MyProducerCallBack implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            if(null != e){
                e.printStackTrace();
                return;
            }
            System.out.println("============MyProducerCallBack============");
            System.out.println("topic:"+recordMetadata.topic());
            System.out.println("分区信息："+recordMetadata.partition());
            System.out.println("偏移量："+recordMetadata.offset());
        }
    }

    public static void main(String[] args) throws Exception {
        //sendMassageForgetResult();
        //sendMassageSync();
        sendMassageCallBack();
    }
}
