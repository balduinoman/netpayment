package net.payment.balance.manager.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.payment.balance.manager.domain.CreditCardAccountBalance;
import net.payment.balance.manager.service.KafkaProducerService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class BalanceManagerController {

    @Autowired
    private KafkaStreams kafkaStreams;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @GetMapping("/api/balance-manager/get-balance-account-by-id")
    public CreditCardAccountBalance getAccountById(@RequestParam String accountId) {

        ReadOnlyKeyValueStore<String, CreditCardAccountBalance> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("balances-state-store", QueryableStoreTypes.keyValueStore())
        );

        return store.get(accountId);
    }

    @PostMapping("/api/balance-manager/create-balance-request")
    public String createRequest(@RequestBody CreditCardAccountBalance creditCardAccountBalance) throws JsonProcessingException {

        String key = UUID.randomUUID().toString();

        // Send the AccountMessage object to Kafka
        kafkaProducerService.sendMessage(key, creditCardAccountBalance);

        return key;
    }
}
