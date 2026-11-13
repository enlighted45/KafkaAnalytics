package com.bank.txn.common.serde;

import org.apache.kafka.common.serialization.Serdes;

/** Kafka Serde for any Jackson-mappable type. */
public class JsonSerde<T> extends Serdes.WrapperSerde<T> {
    public JsonSerde(Class<T> type) {
        super(new JsonSerializer<>(), new JsonDeserializer<>(type));
    }
}
