package com.will.kafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by weiqiang.fu on 2016/8/17.
 */
public class MsgConsumer {
    private static KafkaConsumer<String, String> consumer=null;
    static{
        Properties props = new Properties();
        props.put("bootstrap.servers", "l-test10.dev.cn2.corp.agrant.cn:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String,String>(props);
    }

    public void recvMsg(){
        consumer.subscribe(Arrays.asList("testTopic"));
        int buffersize=10;
        while (true) {
            buffersize--;
            ConsumerRecords<String, String> records = consumer.poll(100);//consumer will fetch all the msgs from broker
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
            if(buffersize<0)
                /* When call it, consumer will commit the offset which is relevant to each msg.
                 * When we try to make it failed, and throw CommitFailedException, consumer will get the duplicate msgs.
                 * And we find that, the offset consumer want to commit is still the pre version.
                  */
                consumer.commitSync();
        }
    }

    public static void main(String[] args){
        MsgConsumer msgConsumer=new MsgConsumer();
        msgConsumer.recvMsg();
    }
}
