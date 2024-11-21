package net.payment.order.manager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.UUID;

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

        creditCardOrdersInputStream.to("orders-output", Produced.with(STRING_SERDE, CREDIT_CARD_ORDER_SERDE));

        // Build and start the KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaStreamsConfiguration.asProperties());
        kafkaStreams.start();

        return kafkaStreams;
    }

    private CreditCardOrder convertJsonToCreditCardOrder(String json)
    {
        try {
            CreditCardOrder creditCardOrder = objectMapper.readValue(json, CreditCardOrder.class);
            creditCardOrder.setOrderId(UUID.randomUUID().toString());
            creditCardOrder.setPaymentStatus("IN-PROGRESS");

            return  creditCardOrder;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
