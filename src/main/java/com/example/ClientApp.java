package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class ClientApp
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );

        Logger logger = LoggerFactory.getLogger(ClientApp.class.getName());

        String bootstrapserver = "127.0.0.1:9092";
        String groupId = "group-1";
        String topic = "test-topic";

        //creating properties

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        try(KafkaConsumer<String,String> kafkaConsumer= new KafkaConsumer<String,String> (properties)){
            kafkaConsumer.subscribe(Arrays.asList(topic));

            while(true){
                kafkaConsumer.poll(Duration.ofSeconds(1))
                        .forEach(record -> {
                            logger.info("key:" + record.key()+ "  " +"value:" + record.value());
                            logger.info("offset:"+ record.offset());
                        });
            }
        }

    }
}
