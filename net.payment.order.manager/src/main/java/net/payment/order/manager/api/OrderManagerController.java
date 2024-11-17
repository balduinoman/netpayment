package net.payment.order.manager.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.payment.order.manager.domain.CreditCardOrder;
import net.payment.order.manager.service.KafkaProducerService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class OrderManagerController {

    @Autowired
    private KafkaStreams kafkaStreams;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @GetMapping("/api/order-manager/get-order-by-id")
    public String getAccountById(@RequestParam String orderRequestId) {

        ReadOnlyKeyValueStore<String, String> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("frauds-state-store", QueryableStoreTypes.keyValueStore())
        );

        return store.get(orderRequestId);
    }

    @PostMapping("/api/order-manager/create-order-request")
    public String createRequest(@RequestBody CreditCardOrder creditCardOrder) throws JsonProcessingException {

        String key = UUID.randomUUID().toString();

        // Send the AccountMessage object to Kafka
        kafkaProducerService.sendMessage(key, creditCardOrder);

        return key;
    }
}
