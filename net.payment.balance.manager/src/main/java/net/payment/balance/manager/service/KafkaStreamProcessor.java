package net.payment.balance.manager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.payment.balance.manager.domain.Account;
import net.payment.balance.manager.domain.CreditCardAccountBalance;
import net.payment.balance.manager.domain.CreditCardOrder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
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
    private static final JsonSerde<CreditCardOrder> CREDIT_CARD_ORDER_SERDE = new JsonSerde<>(CreditCardOrder.class);

    @Bean
    public KafkaStreams kafkaStreams() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //KStream<String, String> accountsIntegrationInputStream = streamsBuilder.stream("accounts-output", Consumed.with(STRING_SERDE, STRING_SERDE));
       // KStream<String, String> balancesIntegrationInputStream = streamsBuilder.stream("balances-input", Consumed.with(STRING_SERDE, STRING_SERDE));
        KStream<String, String> ordersIntegrationInputStream = streamsBuilder.stream("orders-output", Consumed.with(STRING_SERDE, STRING_SERDE));

        //GlobalKTable<String, Account> accountsTable = streamsBuilder.globalTable("account-manager-accounts-state-store-changelog",Consumed.with(STRING_SERDE, ACCOUNT_SERDE));
        //GlobalKTable<String, CreditCardAccountBalance> balancesTable = streamsBuilder.globalTable("balance-manager-balances-state-store-changelog",Consumed.with(STRING_SERDE, CREDIT_CARD_ACCOUNT_BALANCE_SERDE));


        //KStream<String, CreditCardAccountBalance> validatedAccountsIntegrationInputStream = accountsIntegrationInputStream.mapValues(value -> {
       //             try {
       //                 return convertJsonAccountToCreditCardAccountBalance(value);
       //             } catch (Exception e) {
        //                return null;
       //             }
         //       });
//
       // KStream<String, CreditCardAccountBalance> validatedBalancesIntegrationInputStream = balancesIntegrationInputStream.mapValues(value -> {
       //     try {
       //         return convertJsonToCreditCardAccountBalance(value);
       //     } catch (Exception e) {
       //         return null;
       //     }
       // });

        KStream<String, CreditCardOrder> validatedBalancesOrdersIntegrationInputStream = ordersIntegrationInputStream.mapValues(value -> {
            try {
                return convertJsonToCreditCardOrder(value);
            } catch (Exception e) {
                return null;
            }
        });

        KTable<String, CreditCardAccountBalance> balances = validatedBalancesOrdersIntegrationInputStream
                .selectKey((key, value) -> value.getAccountId())
                .groupByKey(Grouped.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE))
                .aggregate(
                        () ->
                        {
                            CreditCardAccountBalance creditCardAccountBalance = new CreditCardAccountBalance();
                            creditCardAccountBalance.setBalance(BigDecimal.ZERO);
                            return creditCardAccountBalance;
                        }, // Initial balance
                        (accountId, transactionAmount, currentBalance) ->
                        {
                            CreditCardAccountBalance creditCardAccountBalance = new CreditCardAccountBalance();
                            creditCardAccountBalance.setCreditCardNumber(transactionAmount.getCreditCardNumber());
                            creditCardAccountBalance.setAccountId(currentBalance.getAccountId());
                            creditCardAccountBalance.setBalance(currentBalance.getBalance().subtract(transactionAmount.getOrderAmount()));
                            return creditCardAccountBalance;
                        },
                        Materialized.<String, CreditCardAccountBalance, KeyValueStore<Bytes, byte[]>>as("balances-state-store")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(CREDIT_CARD_ACCOUNT_BALANCE_SERDE)
                );

        // Build and start the KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaStreamsConfiguration.asProperties());
        kafkaStreams.start();

        return kafkaStreams;
    }

    private CreditCardAccountBalance combineBalances(CreditCardOrder creditCardOrder, CreditCardAccountBalance currentBalance)
    {

        CreditCardAccountBalance creditCardAccountBalance = new CreditCardAccountBalance();

        creditCardAccountBalance.setAccountId(currentBalance.getAccountId());
        creditCardAccountBalance.setCreditCardNumber(currentBalance.getCreditCardNumber());
        creditCardAccountBalance.setBalance(currentBalance.getBalance().subtract(creditCardOrder.getOrderAmount()));

        return creditCardAccountBalance;
    }

    private CreditCardAccountBalance convertJsonAccountToCreditCardAccountBalance(String json)
    {
        try {
            Account account = objectMapper.readValue(json, Account.class);

            CreditCardAccountBalance creditCardAccountBalance = new CreditCardAccountBalance();
            creditCardAccountBalance.setAccountId(account.getAccountId());              // Assuming `getAccountId()` returns a String
            creditCardAccountBalance.setCreditCardNumber(account.getCreditCardNumber());  // Assuming `getCreditCardNumber()` returns a Long
            creditCardAccountBalance.setBalance(new BigDecimal("1000.00"));              // Set default balance, modify as needed
            creditCardAccountBalance.setCreditLimit(new BigDecimal("1000.00"));

            return creditCardAccountBalance;
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

    private CreditCardOrder convertJsonToCreditCardOrder(String json)
    {
        try {
            return objectMapper.readValue(json, CreditCardOrder.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
