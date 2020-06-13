package tez.kafka.elasticsearch;

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
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {
        //Access tab on https://app.bonsai.io/clusters/tez-kafka-lern-8061387299/tokens would have credentials
        //https://b79mt48z1g:r4oistg1wh@tez-kafka-lern-8061387299.ap-southeast-2.bonsaisearch.net:443
        String hostName = "tez-kafka-lern-8061387299.ap-southeast-2.bonsaisearch.net";
        String userName = "b79mt48z1g";
        String password = "r4oistg1wh";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return  client;
    }

    public static KafkaConsumer<String, String> createConsumer() {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        String bootStrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        //https://kafka.apache.org/documentation/#consumerconfigs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        String groupId = "kafka-elasticsearch";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //or latest

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //subscribe consumer to topic
        consumer.subscribe(Arrays.asList("teitter_tweets"));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer();
        //poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    //IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                    //        .source(record.value(), XContentType.JSON);

                    // An Id can be looked at during consuming from Kafka to identify uniqueness of message.
                    //Following Id may be considered if a business unique id is not available in the record
                    //This will help destination application detect a known data and take appropriate action
                    //String uniqueIdOfConsumingRecord = record.topic() + "_" + record.partition() + "_" + record.offset();

                    //Twitter feed specific id
                    String uniqueIdOfConsumingRecord = extractIdFromTweet(record.value());
                    //Because we have set Kafka topic to consume earliest, we would get messages from beginning again
                    //As we extract the tweet id from the message and hand it over to the elasticsearch,
                    //It will help elastic search do a PUT operation instead of POST and avoid duplicates insertion.
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", uniqueIdOfConsumingRecord)
                      .source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info(id);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }finally{
            client.close();
        }
    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String value) {
        //gson library
        return jsonParser.parse(value)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
    //Run from console tab
    //PUT /tweets would create the index
    //GET /twitter/tweets/<IndexResponseID-returned>
    //GET /twitter/tweets/ERQnrnIBmBN2c-s9EWbC
    //ES 7.6 has a limit of 1000 index fields, which you can change in bonsai.io console with
    // PUT /twitter/tweets/_settings body: {"index.mapping.total_fields.limit": 2000}
}
