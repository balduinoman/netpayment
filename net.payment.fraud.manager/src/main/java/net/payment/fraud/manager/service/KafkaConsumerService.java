package net.payment.fraud.manager.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "frauds-output", groupId = "fraud-reporter")
    public void listen(String message) {
        System.out.println("Authorities warned " + message);
    }
}