package com.bank.txn.analytics.stream;

import com.bank.txn.common.model.TransactionEvent;
import com.bank.txn.common.serde.Json;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serde for the raw input topic. The deserializer never throws: invalid JSON or invalid events become a
 * failed {@link ParseResult} carrying the original bytes, so they can be routed to the DLQ instead of
 * stopping the stream (a "poison pill" can never block the partition).
 */
public final class ParseResultSerde {
    private ParseResultSerde() {
    }

    public static Serde<ParseResult> serde() {
        Serializer<ParseResult> ser = (topic, r) -> r == null ? null
                : r.failed() ? r.raw() : Json.MAPPER.writeValueAsBytes(r.event());
        Deserializer<ParseResult> de = ParseResultSerde::parse;
        return Serdes.serdeFrom(ser, de);
    }

    static ParseResult parse(String topic, byte[] data) {
        if (data == null) {
            return new ParseResult(null, null, "null value");
        }
        try {
            TransactionEvent e = Json.MAPPER.readValue(data, TransactionEvent.class);
            String problem = validate(e);
            return problem == null ? new ParseResult(e, null, null) : new ParseResult(null, data, problem);
        } catch (RuntimeException ex) {
            return new ParseResult(null, data, ex.getClass().getSimpleName() + ": " + ex.getMessage());
        }
    }

    static String validate(TransactionEvent e) {
        if (e == null) return "null event";
        if (e.eventId() == null || e.eventId().isBlank()) return "missing eventId";
        if (e.accountId() == null || e.accountId().isBlank()) return "missing accountId";
        if (e.eventTime() == null) return "missing eventTime";
        if (e.amount() == null || e.amount().signum() <= 0) return "missing or non-positive amount";
        if (e.currency() == null || e.type() == null || e.status() == null || e.channel() == null) return "missing enum field";
        return null;
    }
}
