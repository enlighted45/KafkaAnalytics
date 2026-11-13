package com.bank.txn.producer.kafka;

import com.bank.txn.common.Topics;
import com.bank.txn.common.model.TransactionEvent;
import com.bank.txn.producer.generator.EventSender;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

/** Sends events keyed by accountId, so every account's events go to the same partition (ordering). */
public class KafkaEventSender implements EventSender {
    private final KafkaTemplate<String, TransactionEvent> template;

    public KafkaEventSender(KafkaTemplate<String, TransactionEvent> template) {
        this.template = template;
    }

    @Override
    public CompletableFuture<?> send(TransactionEvent event) {
        return template.send(Topics.TRANSACTIONS_RAW, event.accountId(), event);
    }
}
