package tez.kafka.examples2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        //create twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client hosebirdClient= createTwitterClient(msgQueue);
        hosebirdClient.connect();
        //create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("stopping application");
            logger.info("stopping twitter client");
            hosebirdClient.stop();
            logger.info("stopping kafka producer");
            kafkaProducer.close();
            logger.info("done!!");
        }));
        //loop on twitter data feed and feed kafka
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if(msg != null) {
                logger.info(msg);
                //kafka-topics --zookeeper 127.0.0.1:2181 --topic teitter_tweets --create --partitions 6 --replication-factor 1
                //kafka-console-consumer --bootstrap-server localhost:9092 --topic teitter_tweets
                //Tweet something using the word "Kafka" on twitter.com and it will be streamed here to kafka topic
                kafkaProducer.send(new ProducerRecord<>("teitter_tweets", null, msg), (metadata, exception) -> {
                    if(exception != null) {
                        logger.error("Error occurred!!", exception);
                    }
                });
            }
        }
        logger.info("Application ended");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        //create Producer properties
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32 KB batch size

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    String consumerKey = "rKKmJfNBxgHqZEcyDeLXVH1cE";
    String consumerSecret = "XHWDDgV4PaUWhwSYFgu2ZvQO6Jd8tWtmPBpUthDI1J2ZsSY00e";
    String token = "758878868560228352-kqf7R3z1KG2j1H2agqla0CL2MN2rkxO";
    String secret = "dAdLTLeMzLy3vjHDnNwD0AwVso62K1aPsnGTO84RC0YPo";
    //List<String> terms = Lists.newArrayList("bitcoin");
    //List<String> terms = Lists.newArrayList("kafka"); //search words for relevant tweets
    List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");
    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        //https://github.com/twitter/hbc
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //To find out what tweets are to be fed from twitter
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;
    }
}
