package kafka.tutorial1;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //create producer properties.
        //https://kafka.apache.org/documentation/#producerconfigs

        Properties properties = new Properties();

        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer =
                new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<>("first_topic", "hello world");

        //send data - async
        producer.send(producerRecord);

        //clean up
        producer.flush();
        producer.close();
    }
}
