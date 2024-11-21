package net.payment.account.manager.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.payment.account.manager.domain.Account;
import net.payment.account.manager.service.KafkaProducerService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class AccountManagerController {

    @Autowired
    private KafkaStreams kafkaStreams;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @GetMapping("/api/account-manager/get-account-by-id")
    public Account getAccountById(@RequestParam String accountId) {

        ReadOnlyKeyValueStore<String, Account> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("accounts-state-store", QueryableStoreTypes.keyValueStore())
        );

        return store.get(accountId);
    }

    @PostMapping("/api/account-manager/create-request")
    public String createRequest(@RequestBody Account account) throws JsonProcessingException {

        String key = UUID.randomUUID().toString();

        // Send the AccountMessage object to Kafka
        kafkaProducerService.sendMessage(key, account);

        return key;
    }

    @GetMapping("/api/account-manager/get-request-by-id")
    public String getRequestById(@RequestParam String requestId) throws JsonProcessingException {

        ReadOnlyKeyValueStore<String, String> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("requests-state-store", QueryableStoreTypes.keyValueStore())
        );

        return store.get(requestId);
    }
}
