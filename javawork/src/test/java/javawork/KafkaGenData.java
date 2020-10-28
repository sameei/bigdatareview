package javawork;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class KafkaGenData {

    @Test
    public void produceNames() throws Exception {

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(KafkaUtil.producerPropsForStringString());

        producer.send(new ProducerRecord<>(KafkaUtil.testTopic, "?", "Reza")).get();
        producer.send(new ProducerRecord<>(KafkaUtil.testTopic, "?", "Hossein")).get();
        producer.send(new ProducerRecord<>(KafkaUtil.testTopic, "?", "Ali")).get();
        producer.send(new ProducerRecord<>(KafkaUtil.testTopic, "?", "Majid")).get();
    }

}
