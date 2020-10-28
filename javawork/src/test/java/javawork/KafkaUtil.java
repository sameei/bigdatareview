package javawork;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.javatuples.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

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

    public static Properties adminClientProps() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        return props;
    }

    @Test
    public void printTopics() throws Exception {
        listTopics().forEach(i -> {
            System.out.printf("%s\n", i);
        });
    }

    public static List<String> listTopics() throws Exception {
        AdminClient client = AdminClient.create(adminClientProps());
        Collection<TopicListing> ls = client.listTopics().listings().get();
        return ls.stream()
                .map(i -> i.name())
                .filter(i -> !i.startsWith("__"))
                .filter(i -> !i.startsWith("_"))
                .collect(Collectors.toList());
    }


    public static <K,V> List<Pair<K,V>> consumeN(
            Consumer<K,V> consumer, int records) {
        ArrayList<Pair<K,V>> buffer = new ArrayList<>();
        int count = 0;
        while (true) {
            ConsumerRecords<K,V> rs =  consumer.poll(testTimeout);
            for (ConsumerRecord<K,V> r : rs) {
                buffer.add(Pair.with(r.key(), r.value()));
                count+=1;
                if (count >= records) return buffer;
            }
        }
    }

    public static <K,V> List<Pair<K,V>> consumeFor(
            Consumer<K,V> consumer, Duration duration) {
        long start = System.currentTimeMillis();
        long expectedEnd = start + duration.toMillis();
        ArrayList<Pair<K,V>> buffer = new ArrayList<>();
        while(true) {
            ConsumerRecords<K,V> rs = consumer.poll(testTimeout);
            for(ConsumerRecord<K,V> r : rs) {
                buffer.add(Pair.with(r.key(), r.value()));
            }
            if (System.currentTimeMillis() >= expectedEnd) return buffer;
        }
    }

}
