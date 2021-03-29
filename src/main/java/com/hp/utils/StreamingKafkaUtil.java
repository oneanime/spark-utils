package com.hp.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.*;
import scala.Function1;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class StreamingKafkaUtil {
    private static Properties prop = null;
    private static final String CONFIG_FILE_PATH = "kafka.properties";

    private static Map kafkaParams = null;
    static {
        InputStream in = null;
        try {
            prop = new Properties();
            in = SparkJdbcUtil.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH);
            prop.load(in);
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,prop.getProperty("bootstrap.servers"));
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,prop.getProperty("key.deserializer"));
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,prop.getProperty("value.deserializer"));
            kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,prop.getProperty("auto.offset.reset"));
            kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,Boolean.parseBoolean(prop.getProperty("enable.auto.commit")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert in != null;
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static InputDStream<ConsumerRecord<String,String>> getDStream (StreamingContext ssc, List topicSet, String groupId){
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        ConsumerStrategy subscribe = ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams);
        return KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), subscribe);
    }

    public static InputDStream<ConsumerRecord<String,String>> getDStream (StreamingContext ssc, List topicSet, String groupId, Map<TopicPartition, Long> offsets){
        ConsumerStrategy subscribe = ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams, offsets);
        return KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), subscribe);
    }

//    public static DStream<Tuple2> getDStreamWithOffsets(StreamingContext ssc, List topicSet, String groupId, Map<TopicPartition, Long> offsets){
//        InputDStream<ConsumerRecord<String, String>> dStream = getDStream(ssc, topicSet, groupId, offsets);
//        dStream.tr
//
//                .map(rdd -> {
//            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd).offsetRanges();
//            Map<TopicPartition, String> map = Collections.unmodifiableMap(new HashMap<>());
//            for (OffsetRange offsetRange : offsetRanges) {
//                map.put(offsetRange.topicPartition(), offsetRange.topic());
//            }
//            return 1;
//        });
//        new Tuple2<ConsumerRecord<String,String>,Map<TopicPartition,String>>()
//    }

}
