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
    public CreditCardOrder getOrderById(@RequestParam String orderRequestId) {

        ReadOnlyKeyValueStore<String, CreditCardOrder> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("orders-state-store", QueryableStoreTypes.keyValueStore())
        );

        return store.get(orderRequestId);
    }

    @PostMapping("/api/order-manager/create-order-request")
    public CreditCardOrder createRequest(@RequestBody CreditCardOrder creditCardOrder) throws JsonProcessingException {

        String orderId = UUID.randomUUID().toString();
        creditCardOrder.setOrderId(orderId);
        creditCardOrder.setPaymentStatus("CREATED");
        // Send the CreditCardOrder object to Kafka
        kafkaProducerService.sendMessage(orderId, creditCardOrder);

        return creditCardOrder;
    }
}
