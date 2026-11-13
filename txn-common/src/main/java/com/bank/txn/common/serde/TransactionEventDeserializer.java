package com.bank.txn.common.serde;

import com.bank.txn.common.model.TransactionEvent;

/** No-arg deserializer usable from Kafka/Spring configuration properties. */
public class TransactionEventDeserializer extends JsonDeserializer<TransactionEvent> {
    public TransactionEventDeserializer() {
        super(TransactionEvent.class);
    }
}
