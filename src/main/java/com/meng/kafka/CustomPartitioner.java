package com.meng.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * @Description kafka自定义分区分配器
 * @Date 2019/3/28 17:16
 * @Author LSM
 **/
public class CustomPartitioner implements Partitioner {

    /** 定义key的值 **/
    private static final String MESSAGE_KEY = "name";

    /**
     *
     * @param topic
     * @param key
     * @param keyBytes key的字节表示
     * @param value
     * @param valueBytes value的字节表示
     * @param cluster 记录集群信息
     * @return
     */
    @Override
    @SuppressWarnings("all")
    public int partition(String topic,
                         Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes,
                         Cluster cluster) {
        /** 获取分区列表信息 **/
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        /** 获取分区个数 **/
        int numPartitions = partitionInfos.size();
        /** 发送消息必须要有key，否则抛出值错误异常 **/
        if(null == keyBytes || !(key instanceof String)){
            throw new InvalidRecordException("kafka message must have key");
        }
        /** 如果只有一个分区，默认存储 **/
        if(numPartitions ==1){
            return 0;
        }
        /** 如果key是name，发送到最后的分区 **/
        if (MESSAGE_KEY.equals(key)){
            return numPartitions - 1;
        }
        /** kafka默认分配器策略(hash值取余后相同会发送到同一个分区) **/
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

    /**
     * 关闭
     */
    @Override
    public void close() {

    }

    /**
     * 配置
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
