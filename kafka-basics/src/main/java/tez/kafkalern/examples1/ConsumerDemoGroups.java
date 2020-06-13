package tez.kafkalern.examples1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    //No change in code when compared to ConsumerDemo
    //It is just that new group would be able to read all messages again from the topic
    //Launch multiple instances of this program and observe how consumers that are of same group
    // re-balance and sign up for responsibility on exclusive partitions.
    //See with two consumers and then three consumers and then back to two again
    //Use ProducerDemoWithKeys to populate data
    //Observe that a consumer signed up for partition 0 (on re-balancing) would only get messages of partition 0
    //Observe that a consumer signed up for partition 2 (on re-balancing) would only get messages of partition 2
    //Observe that a consumer signed up for partition 0 and 1 (on re-balancing) would only get messages of partition 0 and 1

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
        String bootStrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        //https://kafka.apache.org/documentation/#consumerconfigs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        String groupId = "my-fifth-application";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //or latest

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //subscribe consumer to topic
        consumer.subscribe(Arrays.asList("first_topic"));
        //poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Topic: " + record.topic() + "\n" +
                        "Message: " + record.value());
            }
        }
    }
}
