package net.payment.fraud.manager.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.payment.fraud.manager.domain.CreditCardOrder;
import net.payment.fraud.manager.service.KafkaProducerService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class FraudManagerController {

    @Autowired
    private KafkaStreams kafkaStreams;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @GetMapping("/api/fraud-manager/get-frauds-by-order-id")
    public String getAccountById(@RequestParam String orderRequestId) {

        ReadOnlyKeyValueStore<String, String> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("frauds-state-store", QueryableStoreTypes.keyValueStore())
        );

        return store.get(orderRequestId);
    }

    @PostMapping("/api/fraud-manager/create-order-request")
    public String createRequest(@RequestBody CreditCardOrder creditCardOrder) throws JsonProcessingException {

        String orderId = UUID.randomUUID().toString();

        // Send the AccountMessage object to Kafka
        kafkaProducerService.sendMessage(orderId, creditCardOrder);

        return "Order Id: " + orderId;
    }
}
