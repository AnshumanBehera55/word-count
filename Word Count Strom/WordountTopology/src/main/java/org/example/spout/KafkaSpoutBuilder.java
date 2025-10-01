package org.example.spout;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.example.config.ConfigReader;

public class KafkaSpoutBuilder {




    public static KafkaSpout<String, String> getKafkaSpout(){
        String bootstrapServers = "kafka:29092"; // container-to-container
        String topic = "data-events";
        String groupId = "data-events";

        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setProp("group.id", groupId)
                .setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProp("auto.offset.reset", "earliest")
                .build();

        return new KafkaSpout<>(spoutConfig);
    }
}
