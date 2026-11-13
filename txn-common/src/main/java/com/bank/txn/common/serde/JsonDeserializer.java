package com.bank.txn.common.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final Class<T> type;

    public JsonDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return Json.MAPPER.readValue(data, type);
        } catch (RuntimeException e) {
            throw new SerializationException("Cannot deserialize " + type.getSimpleName() + " from " + topic, e);
        }
    }
}
