package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    public Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static RestHighLevelClient createClient() {
        String hostName = "kafka-course-2743306061.us-east-1.bonsaisearch.net";
        String userName = "ouir778bdb";
        String password = "q1hvyjtadb";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));

        RestClientBuilder restClientBuilder =
                RestClient.builder(new HttpHost(hostName, 443, "https"))
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        });

        return new RestHighLevelClient(restClientBuilder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Collections.singleton(topic));
        return kafkaConsumer;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

        RestHighLevelClient client = createClient();

        String topic = "twitter-tweets";

        KafkaConsumer<String, String> kafkaConsumer = createConsumer(topic);

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

                //2 strategies
                // kafka generic ID
                //String id = consumerRecord.topic()  + "_" + consumerRecord.partition()
                //        + "_" + consumerRecord.offset();
                //twitter feed specific id
                String id = extractIdFromTweet(consumerRecord.value());
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id //this is to make the consumer idempotent
                ).source(consumerRecord.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String respId = indexResponse.getId();
                logger.info("Key: " + consumerRecord.key() + " Value: " + consumerRecord.value());
                logger.info("Metadata. \n" +
                        "Topic: " + consumerRecord.topic() + "\n" +
                        "Partition: " + consumerRecord.partition() + "\n" +
                        "Offset: " + consumerRecord.offset() + "\n" +
                        "TimeStamp: " + consumerRecord.timestamp() + "\n" +
                        "id: " + respId
                );
                Thread.sleep(10000);
            }
        }
        //client.close();
    }

    private static String extractIdFromTweet(String value) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(value)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
