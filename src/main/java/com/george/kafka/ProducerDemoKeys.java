package com.george.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final String KAFKA_SERVER = "localhost:9092";

        final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello-world-" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic, key, value);
            log.info("Key: {}", key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received new metadata. \n" + "Topic: " + recordMetadata.topic() + "\n" + "Partition: " + recordMetadata.partition() + "\n" + "Offset:"
                                + recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            }).get();
        }

        producer.flush();
        producer.close();
    }

}
