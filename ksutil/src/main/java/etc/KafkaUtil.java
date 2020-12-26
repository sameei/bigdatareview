package etc;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class KafkaUtil {

    public static class PropsBuilder {
        private final Properties props;

        public PropsBuilder(Properties props) {
            this.props = props;
        }

        public PropsBuilder() {
            this(new Properties());
        }

        public PropsBuilder put(String k, Object v) {
            props.put(k, v.toString());
            return this;
        }

        public Object get(String k) {
            return props.get(k);
        }

        public Properties result() {
            return props;
        }
    }

    public static PropsBuilder props() {
        return new PropsBuilder();
    }

    public static enum NoOffsetStrategy {
        EARLIEST("earliest"),
        LATEST("latest"),
        ERROR("none");

        private final String value;

        NoOffsetStrategy(String value) {
            this.value = value;
        }

        public String getV() {
            return value;
        }
    }

    private final String name;
    private final String bootstraperServer;

    private final AdminClient admin;

    public KafkaUtil(String name, String bootstraperServer) {
        this.name = name;
        this.bootstraperServer = bootstraperServer;
        this.admin = AdminClient.create(propsAdminClient().result());
    }

    public String newName(String s) {
        return String.format("%s_%s_%d", name, s, System.currentTimeMillis());
    }

    public String getBootstraperServer() {
        return bootstraperServer;
    }

    public String getName() {
        return name;
    }

    protected PropsBuilder propsAdminClient() {
        return props()
                .put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstraperServer)
                .put(AdminClientConfig.CLIENT_ID_CONFIG, newName("kafkaadmin"))
                .put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 5000);
    }

    protected PropsBuilder propsProducer() {
        return props()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstraperServer)
                .put(ProducerConfig.RETRIES_CONFIG, 0)
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.CLIENT_ID_CONFIG, newName("kafkaproducer"));
    }

    public void publish(String topic, List<String> messages) {
        try {
            Producer<String, String> producer =
                    new KafkaProducer<String, String>(propsProducer().result(),
                            new StringSerializer(), new StringSerializer());

            for (String i : messages) {
                producer.send(new ProducerRecord<>(topic, i));
            }
            producer.flush();
            producer.close();
        } catch (Exception cause) {
            throw new RuntimeException(cause);
        }
    }


    protected PropsBuilder propsConsumer() {
        PropsBuilder p = props();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstraperServer);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, newName("kafkaconsumer"));
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return p;
    }

    public List<String> consumeN(String topic, String group, int n, Duration maxWait,
                                 NoOffsetStrategy nofs) {
        try {
            Properties props =
                    propsConsumer()
                            .put(ConsumerConfig.GROUP_ID_CONFIG, group)
                            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, nofs.getV())
                            .result();

            Consumer<String, String> consumer =
                    new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer());

            consumer.subscribe(Collections.singleton(topic));



            ArrayList<String> buf = new ArrayList<>(n);
            int required = n;
            boolean cntu = true;
            long start = System.currentTimeMillis();
            long expectedEnd = start + maxWait.toMillis();

            do {
                Duration timeout =
                        Duration.ofMillis(expectedEnd - System.currentTimeMillis());

                ConsumerRecords<String, String> records =
                        consumer.poll(timeout);

                for (ConsumerRecord<String, String> r : records) {
                    buf.add(r.value());
                    required--;
                    if (required <= 0) break;
                }

                if (required <= 0) cntu = false;
                long timeDiff = expectedEnd - System.currentTimeMillis();
                if (timeDiff <= 0) cntu = false;

            } while (cntu);

            consumer.commitSync();
            consumer.close();

            return buf;
        } catch (Exception cause) {
            throw new RuntimeException(cause);
        }
    }

    public Config createTopic(String name) {
        try {
            return admin.createTopics(Collections.singleton(new NewTopic(name, 1,(short) 1))).config(name).get();
        } catch (Exception cause) {
            throw new RuntimeException(cause);
        }
    }

    public List<String> genAndPublish(String topic, String format, int start, int stop, int step) {
        ArrayList<String> buf = new ArrayList<>();
        for (int i=start; i < stop; i+=step)
            buf.add(String.format(format, i));
        publish(topic, buf);
        return buf;
    }

    public Map<TopicPartition, OffsetAndMetadata> getOffsetsOfGroup(String group) {
        try {
            return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        } catch (Exception cause) {
            throw new RuntimeException(cause);
        }
    }
}
