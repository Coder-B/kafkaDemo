package com.will.kafkaDemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by weiqiang.fu on 2016/8/17.
 */
public class MsgProducer {
    private static Producer<String,String> producer=null;
    static{
        Properties props = new Properties();
        props.put("bootstrap.servers", "l-test10.dev.cn2.corp.agrant.cn:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);//
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String,String>(props);
    }

    public void sendMsg(){
        try {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<String, String>("testTopic", Integer.toString(i), Integer.toString(i)));
                Thread.sleep(10);//Need sleep,otherwise brokers cannot receive msg. It may revolve param "linger.ms".
            }
        }catch (Exception e){

        }finally {
            producer.close();
        }
    }

    public static void main(String[] args){
        MsgProducer msgProducer=new MsgProducer();
        msgProducer.sendMsg();
    }
}
