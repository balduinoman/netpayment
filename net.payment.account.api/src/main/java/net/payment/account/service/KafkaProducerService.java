package net.payment.account.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.payment.account.domain.AccountMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    @Autowired
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicName;
    private final ObjectMapper objectMapper;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
                                @Value("${spring.kafka.template.default-topic}") String topicName,
                                ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
        this.objectMapper = objectMapper;
    }

    public void sendMessage(String key, AccountMessage accountMessage) throws JsonProcessingException {

        // Convert AccountMessage object to JSON
        String message = objectMapper.writeValueAsString(accountMessage);

        // Send the message to Kafka
        kafkaTemplate.send(topicName,key,message);
    }
}