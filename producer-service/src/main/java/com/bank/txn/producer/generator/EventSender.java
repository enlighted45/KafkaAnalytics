package com.bank.txn.producer.generator;

import com.bank.txn.common.model.TransactionEvent;

import java.util.concurrent.CompletableFuture;

/** Abstraction over the Kafka producer so generator logic can be unit tested. */
public interface EventSender {
    CompletableFuture<?> send(TransactionEvent event);
}
