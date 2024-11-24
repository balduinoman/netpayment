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
import org.springframework.kafka.support.serializer.DelegatingSerializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

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
        KStream<String, String> balancesIntegrationInputStream = streamsBuilder.stream("balances-input", Consumed.with(STRING_SERDE, STRING_SERDE));
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
       KStream<String, CreditCardOrder> validatedBalancesIntegrationInputStream = balancesIntegrationInputStream.mapValues(value -> {
            try {
                return convertJsonCreditCardAccountBalanceToCreditCardOrder(value);
            } catch (Exception e) {
                return null;
            }
       });

        KStream<String, CreditCardOrder> validatedBalancesOrdersIntegrationInputStream = ordersIntegrationInputStream.mapValues(value -> {
            try {
                return convertJsonToCreditCardOrder(value);
            } catch (Exception e) {
                return null;
            }
        });

        KTable<String, CreditCardAccountBalance> balances = validatedBalancesOrdersIntegrationInputStream
                .merge(validatedBalancesIntegrationInputStream)
                .selectKey((key, value) -> value.getAccountId())
                .groupByKey(Grouped.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE))
                .aggregate(
                        () ->
                        {
                            CreditCardAccountBalance creditCardAccountBalance = new CreditCardAccountBalance();
                            creditCardAccountBalance.setBalance(BigDecimal.ZERO);
                            return creditCardAccountBalance;
                        }, // Initial balance
                        (accountId, creditCardOrder, currentBalance) ->
                        {
                            CreditCardAccountBalance creditCardAccountBalance = new CreditCardAccountBalance();
                            creditCardAccountBalance.setCreditCardNumber(creditCardOrder.getCreditCardNumber());
                            creditCardAccountBalance.setAccountId(accountId);
                            creditCardAccountBalance.setOrderId(creditCardOrder.getOrderId());
                            creditCardAccountBalance.setCreditLimit(creditCardOrder.getCreditLimit());

                            if(currentBalance.getBalance() == null)
                                currentBalance.setBalance(BigDecimal.ZERO);

                            if(creditCardOrder.getPaymentStatus().equals("ADD_BALANCE"))
                            {
                                creditCardAccountBalance.setBalance(currentBalance.getBalance().add(creditCardOrder.getOrderAmount()));
                                creditCardAccountBalance.setPaymentStatus("EXECUTED");
                            }
                            else
                            {
                                if (currentBalance.getBalance().compareTo(creditCardOrder.getOrderAmount()) >= 0) {
                                    creditCardAccountBalance.setBalance(currentBalance.getBalance().subtract(creditCardOrder.getOrderAmount()));
                                    creditCardAccountBalance.setPaymentStatus("EXECUTED");
                                } else {
                                    creditCardAccountBalance.setBalance(currentBalance.getBalance());
                                    creditCardAccountBalance.setPaymentStatus("NOT_EXECUTED");
                                }
                            }

                            return creditCardAccountBalance;
                        },
                        Materialized.<String, CreditCardAccountBalance, KeyValueStore<Bytes, byte[]>>as("balances-state-store")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(CREDIT_CARD_ACCOUNT_BALANCE_SERDE)
                );

        KStream<String, CreditCardOrder> creditCardOrdersStream = balances
                .toStream()
                .selectKey((key, value) -> value.getOrderId())
                .mapValues(creditCardAccountBalance -> {
                    CreditCardOrder creditCardOrder = new CreditCardOrder();
                    creditCardOrder.setOrderId(creditCardAccountBalance.getOrderId());
                    creditCardOrder.setCreditCardNumber(creditCardAccountBalance.getCreditCardNumber());
                    creditCardOrder.setAccountId(creditCardAccountBalance.getAccountId());
                    creditCardOrder.setOrderAmount(creditCardAccountBalance.getBalance());
                    creditCardOrder.setPaymentStatus(creditCardAccountBalance.getPaymentStatus());
                    creditCardOrder.setCreditLimit(BigDecimal.ZERO);
                    creditCardOrder.setOrderDate(new Date());
                    return creditCardOrder;
                });

        creditCardOrdersStream.to("orders-input", Produced.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE));

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

    private CreditCardOrder convertJsonCreditCardAccountBalanceToCreditCardOrder(String json)
    {
        try {

            CreditCardOrder creditCardOrder = new CreditCardOrder();
            CreditCardAccountBalance creditCardAccountBalance = objectMapper.readValue(json, CreditCardAccountBalance.class);

            creditCardOrder.setAccountId(creditCardAccountBalance.getAccountId());
            creditCardOrder.setCreditCardNumber(creditCardAccountBalance.getCreditCardNumber());
            creditCardOrder.setOrderDate(new Date());
            creditCardOrder.setOrderId(UUID.randomUUID().toString());
            creditCardOrder.setPaymentStatus("ADD_BALANCE");
            creditCardOrder.setOrderAmount(creditCardAccountBalance.getBalance());
            creditCardOrder.setCreditLimit(new BigDecimal("1000.00"));

            return creditCardOrder;

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
