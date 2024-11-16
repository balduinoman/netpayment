package net.payment.balance.manager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.payment.balance.manager.domain.Account;
import net.payment.balance.manager.domain.CreditCardAccountBalance;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

@Component
public class KafkaStreamProcessor {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaStreamsConfiguration kafkaStreamsConfiguration;

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final JsonSerde<Account> ACCOUNT_SERDE = new JsonSerde<>(Account.class);
    private static final JsonSerde<CreditCardAccountBalance> CREDIT_CARD_ACCOUNT_BALANCE_SERDE = new JsonSerde<>(CreditCardAccountBalance.class);

    @Bean
    public KafkaStreams kafkaStreams() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> accountsIntegrationInputStream = streamsBuilder.stream("accounts-integration-output", Consumed.with(STRING_SERDE, STRING_SERDE));
        KStream<String, String> balancesIntegrationInputStream = streamsBuilder.stream("balances-input", Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, CreditCardAccountBalance> validatedAccountsIntegrationInputStream = accountsIntegrationInputStream.mapValues(value -> {
                    try {
                        return convertJsonAccountToCreditCardAccountBalance(value);
                    } catch (Exception e) {
                        return null;
                    }
                });

        KStream<String, CreditCardAccountBalance> validatedBalancesIntegrationInputStream = balancesIntegrationInputStream.mapValues(value -> {
            try {
                return convertJsonToCreditCardAccountBalance(value);
            } catch (Exception e) {
                return null;
            }
        });

        KStream<String, CreditCardAccountBalance> balancesToBeSaved = validatedAccountsIntegrationInputStream.merge(validatedBalancesIntegrationInputStream);

        balancesToBeSaved
                .selectKey((key, value) -> value.getAccountId())
                .groupByKey(Grouped.with(STRING_SERDE, CREDIT_CARD_ACCOUNT_BALANCE_SERDE))
                .reduce(
                        (oldValue, newValue) -> newValue,
                        Materialized.<String, CreditCardAccountBalance, KeyValueStore<Bytes, byte[]>>as("balances-state-store")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(CREDIT_CARD_ACCOUNT_BALANCE_SERDE)
                );

        // Build and start the KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaStreamsConfiguration.asProperties());
        kafkaStreams.start();

        return kafkaStreams;
    }

    private CreditCardAccountBalance convertJsonAccountToCreditCardAccountBalance(String json)
    {
        try {
            Account account = objectMapper.readValue(json, Account.class);

            CreditCardAccountBalance accountBalance = new CreditCardAccountBalance();
            accountBalance.setAccountId(account.getAccountId());              // Assuming `getAccountId()` returns a String
            accountBalance.setCreditCardNumber(account.getCreditCardNumber());  // Assuming `getCreditCardNumber()` returns a Long
            accountBalance.setBalance(new BigDecimal("1000.00"));              // Set default balance, modify as needed
            accountBalance.setCreditLimit(new BigDecimal("1000.00"));

            return accountBalance;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private CreditCardAccountBalance convertJsonToCreditCardAccountBalance(String json)
    {
        try {
            return objectMapper.readValue(json, CreditCardAccountBalance.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    // Define the predicate for Visa
    public static class VisaPredicate implements Predicate<String, Account> {
        @Override
        public boolean test(String key, Account value) {
            return value != null && "Visa".equalsIgnoreCase(value.getBrandName());
        }
    }

    // Define the predicate for MasterCard
    public static class MasterCardPredicate implements Predicate<String, Account> {
        @Override
        public boolean test(String key, Account value) {
            return value != null && "MasterCard".equalsIgnoreCase(value.getBrandName());
        }
    }
}
