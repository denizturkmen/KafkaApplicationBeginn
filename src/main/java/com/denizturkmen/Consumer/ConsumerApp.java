package com.denizturkmen.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {

        String topicName="deniz";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //producerdan farklı olarak group ve client id vermek zorundayız
        // het .alıştığında değiştir
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"groupId2");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"clientId2");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.println(record.value());
            }
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
        kafkaConsumer.close();
    }
}
