package kafka.tutorial1;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        String topic = "first_topic";

        //create producer record
        for (int i = 0; i < 10; i++) {

            String val = "hello world" + i;
            String key = "id_" + i;

            //same key always goes to the same partition always

            logger.info("Key: " + key);

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, val);

            //send data - async
            producer.send(producerRecord, (recordMetadata, e) -> {
                //executes everytime record is successfully sent or exception is thrown
                if (e == null) {
                    // record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "TimeStamp: " + recordMetadata.timestamp() + "\n"
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); //block the .send() to make it sync - don't do this in prod!!
        }

        //clean up
        producer.flush();
        producer.close();
    }
}
