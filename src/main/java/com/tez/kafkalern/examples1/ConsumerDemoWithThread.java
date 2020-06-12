package com.tez.kafkalern.examples1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String topic = "first_topic";
        String groupId = "my-sixth-application";
        String bootStrapServers = "127.0.0.1:9092";
        int numberOfThreads = 1;
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        logger.info("Creating consumer runnable");
        //just one runnable
        Runnable myConRunnable = new ConsumerRunnable(bootStrapServers,topic, groupId, latch);
        Thread consumerThread = new Thread(myConRunnable);
        logger.info("Staring consumer thread");
        consumerThread.start();

        //If the main program is terminated, we want to inform child threads and wait till all of then latch down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            //just one runnable
            ((ConsumerRunnable)myConRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited!");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Error!!! :(", e);
        } finally {
            logger.info("Program terminated");
        }
    }
}

class ConsumerRunnable implements Runnable {

    private final CountDownLatch latch;
    //create consumer
    KafkaConsumer<String, String> consumer;
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    public ConsumerRunnable(String bootStrapServers, String topic, String groupId, CountDownLatch latch) {
        this.latch = latch;
        Properties properties = new Properties();
        //https://kafka.apache.org/documentation/#consumerconfigs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //or latest
        consumer = new KafkaConsumer<String, String>(properties);
        //subscribe consumer to topic
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        //poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n" +
                            "Topic: " + record.topic() + "\n" +
                            "Message: " + record.value());
                }
            }
        } catch(WakeupException wuex) {
            logger.info("Received shutdown signal!!");
        } finally {
            consumer.close();
            //tell the parent code that this thread is done.
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup(); //interrupts consumer.poll() and this leads to WakeupException
    }
}