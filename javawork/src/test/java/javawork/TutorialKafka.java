package javawork;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

public class TutorialKafka {

    @Test
    public void producerSample() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "???");
        // Producer<String, String> producer = new KafkaProducer<String, String>(props);
        Producer<String, String> producer = new KafkaProducer<String, String>(
                props, new StringSerializer(), new StringSerializer());
        producer.send(new ProducerRecord<>("topic-name", "Karaj", "Reza")).get();


        Main.main(new String[]{"127.0.0.1", "9092", "ALL"});
    }

    @Test
    public void simpleJavaCore() throws Exception {
    }
}
