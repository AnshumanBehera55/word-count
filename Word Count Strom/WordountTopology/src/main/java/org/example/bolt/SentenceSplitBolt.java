package org.example.bolt;

import org.apache.commons.lang.ObjectUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SentenceSplitBolt extends BaseRichBolt {
    OutputCollector collector;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        System.out.println("started sentence");
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("Data Entered:" +tuple.toString());
        String sentence= tuple.getStringByField("value");
        String[] splitted_words=sentence.split(" ");
        List<String> words=Arrays.stream(splitted_words)
                .filter(word->word != null && word!="")
                .collect(Collectors.toList());
        words.forEach(word->{
            collector.emit(new Values(word));

        });
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));

    }
}
