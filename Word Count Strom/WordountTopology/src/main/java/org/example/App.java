package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.example.bolt.ReportBolt;
import org.example.bolt.SentenceSplitBolt;
import org.example.bolt.WordCountBolt;
import org.example.spout.KafkaSpoutBuilder;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", KafkaSpoutBuilder.getKafkaSpout(),1);

        builder.setBolt("sentenceSplitBolt", new SentenceSplitBolt(),4)
                .shuffleGrouping("kafkaSpout");

        builder.setBolt("wordCountBolt",new WordCountBolt(),4)
                .fieldsGrouping("sentenceSplitBolt", new Fields("word"));

        builder.setBolt("reportBolt", new ReportBolt(),1)
                .globalGrouping("wordCountBolt");



        Config conf = new Config();
        conf.put(Config.NIMBUS_SEEDS, Arrays.asList("nimbus"));
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.setNumWorkers(2);
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            try {
                System.out.println("Started app");
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Thread.sleep(Long.MAX_VALUE*1000*60*60);
        }

    }
}
