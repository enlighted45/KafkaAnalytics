package com.bank.txn.common;

import com.bank.txn.common.model.*;
import com.bank.txn.common.serde.JsonSerde;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionEventSerdeTest {

    private final JsonSerde<TransactionEvent> serde = new JsonSerde<>(TransactionEvent.class);

    private static TransactionEvent sample() {
        return new TransactionEvent("e-1", "ACC-000001", 42, TransactionType.PAYMENT, Channel.POS,
                TransactionStatus.APPROVED, new BigDecimal("1234.50"), Currency.USD, "GROCERY", Region.NORTH,
                Instant.parse("2026-11-12T10:15:30.123Z"), Instant.parse("2026-11-12T10:15:31.004Z"));
    }

    @Test
    void roundTripPreservesAllFieldsIncludingBigDecimalAndInstants() {
        byte[] bytes = serde.serializer().serialize("t", sample());
        TransactionEvent back = serde.deserializer().deserialize("t", bytes);
        assertThat(back).isEqualTo(sample());
    }

    @Test
    void timestampsAreWrittenAsIsoStrings() {
        String json = new String(serde.serializer().serialize("t", sample()), StandardCharsets.UTF_8);
        assertThat(json).contains("\"eventTime\":\"2026-11-12T10:15:30.123Z\"");
        assertThat(json).doesNotContain("amountInr").doesNotContain("\"approved\"");
    }

    @Test
    void unknownFieldsAreIgnored() {
        String json = new String(serde.serializer().serialize("t", sample()), StandardCharsets.UTF_8)
                .replace("{", "{\"newField\":1,");
        assertThat(serde.deserializer().deserialize("t", json.getBytes(StandardCharsets.UTF_8))).isEqualTo(sample());
    }

    @Test
    void malformedJsonRaisesSerializationException() {
        assertThatThrownBy(() -> serde.deserializer().deserialize("t", "not-json".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class);
    }

    @Test
    void convertsToInr() {
        assertThat(sample().amountInr()).isEqualByComparingTo("103080.75");
    }
}
