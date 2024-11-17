package net.payment.order.manager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.payment.order.manager.domain.Account;
import net.payment.order.manager.domain.CreditCardAccountBalance;
import net.payment.order.manager.domain.CreditCardOrder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
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
    private static final JsonSerde<Account> ACCOUNT_SERDE = new JsonSerde<>(Account.class);
    private static final JsonSerde<CreditCardAccountBalance> CREDIT_CARD_ACCOUNT_BALANCE_SERDE = new JsonSerde<>(CreditCardAccountBalance.class);

    @Bean
    public KafkaStreams kafkaStreams() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> stream = streamsBuilder.stream("orders-input", Consumed.with(STRING_SERDE, STRING_SERDE));
        GlobalKTable<String, Account> accountsTable = streamsBuilder.globalTable("account-manager-accounts-state-store-changelog",Consumed.with(STRING_SERDE, ACCOUNT_SERDE));
        GlobalKTable<String, CreditCardAccountBalance> balancesTable = streamsBuilder.globalTable("balance-manager-balances-state-store-changelog",Consumed.with(STRING_SERDE, CREDIT_CARD_ACCOUNT_BALANCE_SERDE));

        KStream<String, CreditCardOrder> creditCardOrdersInputStream = stream.mapValues(value -> {
            try {
                return convertJsonToCreditCardOrder(value);
            } catch (Exception e) {
                return null;
            }
        });

        // Join the KStream with the GlobalKTable
        KStream<String, CreditCardOrder> ordersAccount = creditCardOrdersInputStream.join(
                accountsTable,
                (orderKey, orderValue) -> orderValue.getAccountId(), // Extract productId from order
                (order, account) -> order
        );

        // Join the KStream with the GlobalKTable
        KStream<String, CreditCardOrder> ordersBalances = ordersAccount.join(
                balancesTable,
                (orderKey, orderValue) -> orderValue.getAccountId(), // Extract productId from order
                (order, balance) -> (order.getOrderAmount().compareTo(balance.getBalance()) < 0) ? order : null
        );

        ordersBalances.to("orders-commited", Produced.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE));

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
