package net.payment.account.manager.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.payment.account.manager.domain.Account;
import net.payment.account.manager.service.KafkaProducerService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.Objects;
import java.util.UUID;

@RestController
public class AccountManagerController {

    private final StreamsBuilderFactoryBean factoryBean;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    public AccountManagerController(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @GetMapping("/api/account-manager/get-account-by-id")
    public Account getAccountById(@RequestParam String accountId) {

        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

        ReadOnlyKeyValueStore<String, Account> store = Objects.requireNonNull(kafkaStreams).store(
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

        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

        ReadOnlyKeyValueStore<String, String> store = Objects.requireNonNull(kafkaStreams).store(
                StoreQueryParameters.fromNameAndType("requests-state-store", QueryableStoreTypes.keyValueStore())
        );

        return store.get(requestId);
    }
}
