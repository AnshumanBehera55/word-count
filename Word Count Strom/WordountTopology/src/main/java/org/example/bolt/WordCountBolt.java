package org.example.bolt;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts;
    private KafkaProducer<String, String> producer;
    private final String outputTopic="processed-data";

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        counts=new HashMap<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple tuple) {

        String word=tuple.getStringByField("word");
        counts.put(word,counts.getOrDefault(word,0)+1);
        collector.emit(new Values(word, counts.get(word)));
        String msg = word + ":" + counts.get(word);
        producer.send(new ProducerRecord<>(outputTopic, msg));
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       outputFieldsDeclarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
        if (producer != null) {
            producer.close();
        }
    }

}
