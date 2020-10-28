package javawork;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Properties;

public class KafkaUtil {

    public static final Duration testTimeout = java.time.Duration.ofSeconds(2);
    public static final String kafkaServers = "127.0.0.1:9092";
    public static final String testTopic = "testTopic-01";


    public static Properties producerProps() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty("retries", "0");
        return props;
    }

    public static Properties producerPropsForStringString() {
        Properties props = producerProps();
        props.setProperty(
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static Properties consumerProps() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.setProperty("enable.auto.commit", "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest, earliest, none
        return props;
    }

    public static Properties consumerPropsStringString(String groupId) {
        Properties props = new Properties();
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static void consumeN(String topicName, int records) {
        
    }

}
