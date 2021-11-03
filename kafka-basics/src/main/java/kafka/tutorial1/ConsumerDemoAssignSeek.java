package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        logger.info("Starting Consumer");

        //https://kafka.apache.org/documentation/#consumerconfigs

        String bootStrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        //String groupId = "my-fifth-application";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //subscribe consumer to our topic(s)
        //kafkaConsumer.subscribe(Collections.singleton(topic));

        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offSetToReadFrom = 15L;
        kafkaConsumer.assign(Arrays.asList(topicPartition));

        //seek
        kafkaConsumer.seek(topicPartition, offSetToReadFrom);

        int numOfMsgToRead = 5;
        boolean keepOnReading = true;
        int numOfMsgReadSoFar = 0;

        //poll for new data

        while (keepOnReading) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for (ConsumerRecord<String,String> consumerRecord : consumerRecords) {
                logger.info("Key: " + consumerRecord.key() + " Value: " + consumerRecord.value());
                logger.info("Metadata. \n" +
                        "Topic: " + consumerRecord.topic() + "\n" +
                        "Partition: " + consumerRecord.partition() + "\n" +
                        "Offset: " + consumerRecord.offset() + "\n" +
                        "TimeStamp: " + consumerRecord.timestamp() + "\n"
                );
                if (numOfMsgReadSoFar >= numOfMsgToRead){
                    keepOnReading = false;
                    break;
                }
                numOfMsgReadSoFar++;
            }
            logger.info("Existing the application");
        }
    }
}
