package net.payment.fraud.manager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.payment.fraud.manager.domain.CreditCardOrder;
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

@Component
public class KafkaStreamProcessor {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaStreamsConfiguration kafkaStreamsConfiguration;

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final JsonSerde<CreditCardOrder> CREDIT_CARD_ORDER_SERDE = new JsonSerde<>(CreditCardOrder.class);

    @Bean
    public KafkaStreams kafkaStreams() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> fraudsInputStream = streamsBuilder.stream("frauds-input", Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, CreditCardOrder> validatedBalancesIntegrationInputStream = fraudsInputStream.mapValues(value -> {
            try {
                return convertJsonToCreditCardOrder(value);
            } catch (Exception e) {
                return null;
            }
        });

        // Define the predicates
        Predicate<String, CreditCardOrder> fraudPredicate = new FraudPredicate();
        Predicate<String, CreditCardOrder> notFraudPredicate = new NotFraudPredicate();

        // Branch the stream
        KStream<String, CreditCardOrder>[] branches = validatedBalancesIntegrationInputStream
             .filter((key, value) -> value != null)
             .branch(fraudPredicate, notFraudPredicate);

        KStream<String, String> validOrderRequestStream =  branches[0].mapValues(value -> "Not Fraud:" + value.getOrderId());
        KStream<String, String> invalidOrderRequestAccountStream =  branches[1].mapValues(value -> "Fraud" + value.getOrderId());
        KStream<String, String> validatedOrderRequestsStream = invalidOrderRequestAccountStream.merge(validOrderRequestStream);

        validatedOrderRequestsStream
                .groupByKey(Grouped.with(STRING_SERDE, STRING_SERDE))
                .reduce(
                        (oldValue, newValue) -> newValue,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("frauds-state-store")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(STRING_SERDE)
                );

        branches[1].to("orders-input", Produced.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE));
        branches[0].to("frauds-output", Produced.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE));

        // Build and start the KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaStreamsConfiguration.asProperties());
        kafkaStreams.start();

        return kafkaStreams;
    }

    private CreditCardOrder convertJsonToCreditCardOrder(String json)
    {
        try {
            return objectMapper.readValue(json, CreditCardOrder.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    // Define the predicate for Visa
    public static class NotFraudPredicate implements Predicate<String, CreditCardOrder> {
        @Override
        public boolean test(String key, CreditCardOrder value) {
            return value != null && !value.getAccountId().startsWith("5");
        }
    }

    // Define the predicate for MasterCard
    public static class FraudPredicate implements Predicate<String, CreditCardOrder> {
        @Override
        public boolean test(String key, CreditCardOrder value) {
            return value != null && value.getAccountId().startsWith("5");
        }
    }
}
