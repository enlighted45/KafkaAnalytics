package com.bank.txn.common.serde;

import com.bank.txn.common.model.TransactionEvent;

/** No-arg serializer usable from Kafka/Spring configuration properties. */
public class TransactionEventSerializer extends JsonSerializer<TransactionEvent> {
}
