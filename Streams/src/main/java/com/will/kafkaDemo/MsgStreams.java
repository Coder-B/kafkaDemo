package com.will.kafkaDemo;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

/**
 * Created by 11654 on 2016/8/21.
 */
public class MsgStreams {
    private static StreamsConfig config=null;
    private static final Properties props = new Properties();
    static {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");//StreamsConfig.STATE_DIR_CONFIG
        props.put(StreamsConfig.STATE_DIR_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "l-test10.dev.cn2.corp.agrant.cn:9092,l-test10.dev.cn2.corp.agrant.cn:9093");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config = new StreamsConfig(props);
    }

    private void streamsToTopics() throws InterruptedException{
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,String> source=builder.stream("testTopic");
        KTable<String,Long> counts= source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String s) {
                return Arrays.asList(s.toLowerCase(Locale.getDefault()).split(" "));
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(String s, String s2) {
                System.out.println(s2);
                return new KeyValue<>(s2, s2);
            }
        }).countByKey("Counts");
        counts.to(Serdes.String(), Serdes.Long(), "streams-wordcount-output");
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(5000L);

        streams.close();
    }

    public static void main(String[] args){
        try {
            new MsgStreams().streamsToTopics();
        }catch (InterruptedException e){

        }
    }
}
