package net.payment.order.manager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.payment.order.manager.domain.CreditCardOrder;
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

        KStream<String, String> stream = streamsBuilder.stream("orders-input", Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, CreditCardOrder> creditCardOrdersInputStream = stream.mapValues(value -> {
            try {
                return convertJsonToCreditCardOrder(value);
            } catch (Exception e) {
                return null;
            }
        });

        KStream<String, CreditCardOrder> ordersInProgress = creditCardOrdersInputStream
                .filter(
                        (key, value) ->
                                (value != null && value.getPaymentStatus() != null && value.getPaymentStatus().equals("NOT_FRAUD"))
                )
                .mapValues((value) ->
                {
                    value.setPaymentStatus("PROGRESS");
                    return value;
                } );

        KStream<String, CreditCardOrder> ordersFinished = creditCardOrdersInputStream
                .filter(
                        (key, value) ->
                                (value != null
                                        && value.getPaymentStatus() != null
                                        && (value.getPaymentStatus().equals("EXECUTED") || value.getPaymentStatus().equals("NOT_EXECUTED")))
                )
                .mapValues((key, value) ->
                {
                    value.setPaymentStatus("COMPLETED-" + value.getPaymentStatus());
                    return value;
                });

        creditCardOrdersInputStream
                .groupByKey()
                .aggregate(
                        CreditCardOrder::new, // Initial balance
                        (orderId, newCreditCardOrder, currentCreditCardOrder) ->
                        {
                            if (currentCreditCardOrder == null) {
                                currentCreditCardOrder = new CreditCardOrder();
                            }

                            currentCreditCardOrder.setOrderId(orderId);

                            if(currentCreditCardOrder.getAccountId() == null)
                                currentCreditCardOrder.setAccountId(newCreditCardOrder.getAccountId());

                            if(currentCreditCardOrder.getCreditCardNumber() == null)
                                currentCreditCardOrder.setCreditCardNumber(newCreditCardOrder.getCreditCardNumber());

                            if(currentCreditCardOrder.getOrderAmount() == null)
                                currentCreditCardOrder.setOrderAmount(newCreditCardOrder.getOrderAmount());

                            if(currentCreditCardOrder.getOrderDate() == null)
                                currentCreditCardOrder.setOrderDate(newCreditCardOrder.getOrderDate());

                            if(currentCreditCardOrder.getOrderDate() == null)
                                currentCreditCardOrder.setCreditLimit(newCreditCardOrder.getCreditLimit());

                            currentCreditCardOrder.setPaymentStatus(newCreditCardOrder.getPaymentStatus());

                            return currentCreditCardOrder;
                        },
                        Materialized.<String, CreditCardOrder, KeyValueStore<Bytes, byte[]>>as("orders-state-store")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(CREDIT_CARD_ORDER_SERDE)
                );

        ordersInProgress.to("orders-output", Produced.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE));

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
}
