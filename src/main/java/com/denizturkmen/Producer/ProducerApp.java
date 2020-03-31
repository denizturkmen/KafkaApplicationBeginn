package com.denizturkmen.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    public static void main(String[] args) throws InterruptedException {

        String topicName = "deniz";
        String[] names = {"Besra", "Mehmet", "Ilkcan", "Melis","Asli","Merve","Besra", "Mehmet", "Ilkcan", "Melis","Asli","Merve"};
        Properties properties = new Properties();
        //kafka konumu remote ip ve port verebilirsin
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // kafka verileri byte[] olarak kabul eder ondan serialize ve deserialize et
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //Mesaj göndermek için
        Producer producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < names.length; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, names[i]);
            producer.send(record);
            TimeUnit.SECONDS.sleep(2);
        }
        System.out.println("Producer Record Ok!");
        producer.close();

    }
}
