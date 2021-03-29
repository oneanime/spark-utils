package com.hp.utils;

import org.apache.kafka.common.TopicPartition;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class OffsetManagerUtil {

    public static Map<TopicPartition, Long> getFromRedis(String groupId, String topic) {
        Jedis jedis = RedisUtils.create().getJedis();
        Map<String, String> offsetMap = jedis.hgetAll(groupId + "->" + topic);
        jedis.close();
        if (offsetMap.isEmpty()) {
            return null;
        } else {
            HashMap<TopicPartition, Long> result = new HashMap<>();
            for (Map.Entry<String, String> entry : offsetMap.entrySet()) {
                int partitionNum = Integer.parseInt(entry.getKey());
                long offset = Long.parseLong(entry.getValue());
                TopicPartition topicPartition = new TopicPartition(topic,partitionNum);
                result.put(topicPartition, offset);
            }
            return result;
        }
    }

    public static void saveToRedis(String groupId, Map<TopicPartition, Long> offsets) {
        Jedis jedis = RedisUtils.create().getJedis();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Long offset = entry.getValue();
            String partition = String.valueOf(topicPartition.partition());
            String topic = topicPartition.topic();
            HashMap<String, String> value = new HashMap<>();
            value.put(partition,String.valueOf(offset));
            jedis.hset(groupId + "->" + topic, value);
        }
        jedis.close();
    }
}
