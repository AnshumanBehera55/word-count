package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException, IOException {

        String bootstrapServers = "localhost:9092";
        String topic = "data-events";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try (InputStream in = App.class.getResourceAsStream("/sentence.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            List<String> lines = reader.lines().collect(Collectors.toList());

            for (String line : lines) {
                if (line != null && !line.isBlank()) {  // avoid null/empty lines
                    producer.send(new ProducerRecord<>(topic, null, line));
                    System.out.println("Sent: " + line);
                    Thread.sleep(1000); // simulate streaming
                }
            }
        }

        producer.close();
    }


}
