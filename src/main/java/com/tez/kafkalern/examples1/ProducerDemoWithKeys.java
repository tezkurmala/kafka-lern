package com.tez.kafkalern.examples1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getName());
        //create Producer properties
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) { //messages will be sent 3 to first partion, 4 to second, 3 to third like round robin - we have 3 partitions for this topic
            String topic = "first_topic";
            String message = "programmed hello world: " + i;
            //same key assures producing to same partition.
            // Key helps producer to post always to same partition and hence consumer consuming from that partition would get messages in sequence.
            String key = "id_" + i;
            //create producer record having data to publish
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", key, message);
            //send() does in async but for now we will block using get() over send() just to find out which Key is going to which Partition.
            //Blocking until each message is send and callback is triggered before proceeding to publish next message
            //Just for learning - Not recommended in Prod
            //The callback metadata does not give the Key - Hence this roundabout to learn
            logger.info("Key: " + key + " for message " + message);
            producer.send(record, (metadata, exception) -> {
                //executed every time each record is sent or failed
                if (exception == null) {
                    //success
                    logger.info("Received new metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    logger.error("Error ", exception);
                }
            }).get(); //Do not loop and send next message. Wait until the current message is sent and we know ehich partition it is posted to.
                      // Adding get() to block until the send is complete just to print one after the other and understand.
        }
        // Key id_0 is put to Partition 1
        // Key id_1 is put to Partition 0
        // Key id_2 is put to Partition 2
        // Key id_3 is put to Partition 0
        // Key id_4 is put to Partition 2
        // Key id_5 is put to Partition 2
        // Key id_6 is put to Partition 0
        // Key id_7 is put to Partition 2
        // Key id_8 is put to Partition 1
        // Key id_9 is put to Partition 2
        //Any number of times we run this program or use a same Key with new messages, they end up in same partition.
        //This is because the Key as input is hashed to determine which partition it has to go to.
        //If we increase the partitions, the outcome would change because now we ave more or less partitions.

        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
