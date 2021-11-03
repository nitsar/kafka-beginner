package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer() {

    }

    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();
    }

    public void run() throws InterruptedException {
        //create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        Client hosebirdClient = createTwitterClient(msgQueue);
        hosebirdClient.connect();

        //create kafka producer
        KafkaProducer<String,String> kafkaProducer = createKafkaProducer();

        //add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping applicaiton");
            logger.info("shutting down client from twitter");
            hosebirdClient.stop();
            kafkaProducer.close();
            logger.info("done");
        }));

        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                logger.info(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if (msg != null) {
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad");
                        }
                    }
                });
            }
        }
        //loop to send tweets to kafka

        hosebirdClient.stop();
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) throws InterruptedException {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);

/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("silchar");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("5Slzpvoz8Z5R3ATVDdn1iGpPZ",
                "r2cJyoh2lCsTLzOnq5MsIvDJwqfu1DGRjFigQrpgySmaTikZOv",
                "84567644-Bib0HSDTQK2Ie17XtOcTGQQXTl1TNgEnWvrgp151Y",
                "utp5JN3UOFCc1kqQMo3AbYmcSnbPARYURwpAcmeGnq0vK");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        return builder.build();
    }

    public KafkaProducer<String,String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer.
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //create producer
        return new KafkaProducer<>(properties);
    }
}
