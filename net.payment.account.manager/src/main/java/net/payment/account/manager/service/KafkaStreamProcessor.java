package net.payment.account.manager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.payment.account.manager.domain.Account;
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

    @Bean
    public KafkaStreams kafkaStreams() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> stream = streamsBuilder.stream("accounts-input", Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, Account> accountStream = stream.mapValues(value -> {
                    try {
                        return convertJsonToAccount(value);
                    } catch (Exception e) {
                        return null;
                    }
                });

        // Define the predicates
        Predicate<String, Account> visaPredicate = new VisaPredicate();
        Predicate<String, Account> mastercardPredicate = new MasterCardPredicate();

        // Branch the stream
        KStream<String, Account>[] branches = accountStream
                .filter((key, value) -> value != null)
                .branch(visaPredicate, mastercardPredicate);

        KTable<String, Account> accountTable =  branches[0]
                //.filter((key, value) -> value != null)
                .selectKey((key, value) -> value.getAccountId())
                .groupByKey(Grouped.with(STRING_SERDE, ACCOUNT_SERDE))
                .reduce(
                        (oldValue, newValue) -> newValue, // Replace old value with the new one
                        Materialized.<String, Account, KeyValueStore<Bytes, byte[]>>as("accounts-state-store")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(ACCOUNT_SERDE)
                );

        accountTable.toStream().to("accounts-integration-output", Produced.with(STRING_SERDE, ACCOUNT_SERDE));

        KStream<String, String> validRequestAccountStream =  branches[0].mapValues(value -> "Account id:" + value.getAccountId());
        KStream<String, String> invalidRequestAccountStream =  branches[1].mapValues(value -> "Invalid Request");
        KStream<String, String> validatedRequestsAccountStream = invalidRequestAccountStream.merge(validRequestAccountStream);

        validatedRequestsAccountStream
                .groupByKey(Grouped.with(STRING_SERDE, STRING_SERDE))
                .reduce(
                        (oldValue, newValue) -> newValue,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("requests-state-store")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(STRING_SERDE)
                );

        // Route invalid messages to DLQ
        KStream<String, String> dlqStream = stream.filter((key, value) -> {
            try {
                convertJsonToAccount(value);
                return false;
            } catch (Exception e) {
                return true;
            }
        });

        dlqStream.to("accounts-dlq");

        // Build and start the KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaStreamsConfiguration.asProperties());
        kafkaStreams.start();

        return kafkaStreams;
    }

    private Account convertJsonToAccount(String json)
    {
        try {
            Account account = objectMapper.readValue(json, Account.class);
            account.setAccountLastUpdate(LocalDateTime.now());
            return account;
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