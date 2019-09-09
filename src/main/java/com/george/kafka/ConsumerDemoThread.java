package com.george.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {

    private ConsumerDemoThread() {
    }

    public static void main(String[] args) {
        new ConsumerDemoThread().run();
    }

    private void run() {
        Logger log = LoggerFactory.getLogger(ConsumerDemoThread.class);

        String servers = "localhost:9092";
        String groupId = "my-fourth-group";
        String offsetReset = "earliest";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        log.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, servers, groupId, topic);
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shoutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("The application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application is interrupted", e);
        } finally {
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;

        public ConsumerRunnable(final CountDownLatch latch, String servers, String groupId, String topic) {
            this.latch = latch;
            String offsetReset = "earliest";

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
            consumer = new KafkaConsumer<String,String>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String,String> record : records) {
                        log.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Received shutdown signal!!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
