package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        System.out.println("hello world");

        Logger logger = LoggerFactory.getLogger(Producer.class.getName());

        String bootstrapserver = "127.0.0.1:9092";
        String topic = "test-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)){

            for(Integer i=1;i<10;i++){
                ProducerRecord<String, String> record = new ProducerRecord<>(topic,i.toString());
                kafkaProducer.send(record);
                Thread.sleep(1000);
            }
            kafkaProducer.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
