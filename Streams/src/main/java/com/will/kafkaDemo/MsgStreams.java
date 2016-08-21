package com.will.kafkaDemo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 11654 on 2016/8/21.
 */
public class MsgStreams {
    private static StreamsConfig config=null;
    static {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_DOC, Serdes.StringSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_DOC, Serdes.StringSerde.class);
        config = new StreamsConfig(props);
    }

    private void streamsToTopics(){
        KStreamBuilder builder = new KStreamBuilder();
        builder..from("my-input-topic").mapValue(value -> value.length().toString()).to("my-output-topic");
    }
}
