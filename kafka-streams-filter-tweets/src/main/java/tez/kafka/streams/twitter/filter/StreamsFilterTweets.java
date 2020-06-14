package tez.kafka.streams.twitter.filter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams"); //like consumer group id
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("teitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                //filter for tweets which has a user of over 10000 followers
                (key, tweet)-> extractUserFollowersFromTweet(tweet) > 10000
        );
        filteredStream.to("important_tweets");
        //kafka-topics --zoo-keeper localhost:2181 --topic important_tweets --create --partitions 3 --replication-factor 1

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        //start our streams application
        kafkaStreams.start();

        //Now run the kafka-twitter-producer to pull tweets from twitter and publish to teitter_tweets topic
        //This Kafka streams application will filter those tweets and copy to important_tweets topic
        //kafka-console-consumer --bootstrap-server localhost:9092 --topic important_tweets --from-beginning

    }

    private static JsonParser jsonParser = new JsonParser();
    private static int extractUserFollowersFromTweet(String tweetJson) {
        try {
            //gson library
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception e) {
            return 0;
        }
    }
}
