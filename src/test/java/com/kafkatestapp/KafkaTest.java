package com.kafkatestapp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

public class KafkaTest {

    @DisplayName("ProducerTest")
    @Test
    void producer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 900; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("send_sms", "send " + i);
            producer.send(record);
        }

        producer.close();
    }

    @DisplayName("ConsumerTest")
    @Test
    void consumerTest() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Consumer Group 설정
        props.put("group.id", "log_group");

        // Consumer 객체 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Polling을 감지할 Topic을 설정
        consumer.subscribe(List.of("click_log"));

        // 무한 반복
        // 500ms 마다 Topic에 쌓인 레코드를 가져온다.
        ConsumerRecords<String, String> records = consumer.poll(500);

        // Topic에 쌓인 Record 배열을 받아 처리
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Data: " + record.value());
        }
    }
}
