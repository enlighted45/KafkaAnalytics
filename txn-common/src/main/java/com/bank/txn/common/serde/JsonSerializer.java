package com.bank.txn.common.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {
    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return Json.MAPPER.writeValueAsBytes(data);
        } catch (RuntimeException e) {
            throw new SerializationException("Cannot serialize value for topic " + topic, e);
        }
    }
}
