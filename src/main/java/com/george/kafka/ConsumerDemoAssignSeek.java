package com.george.kafka;

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

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String servers = "localhost:9092";
        String offsetReset = "earliest";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        long offsetToReadFrom = 2L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int nbOfMsgToRead = 5;
        boolean keepOnReading = true;
        int nbOfMsgRead = 0;

        while (keepOnReading) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record : records) {
                log.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
                nbOfMsgRead += 1;
                if (nbOfMsgRead >= nbOfMsgToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        log.info("Exiting the application");
    }
}
