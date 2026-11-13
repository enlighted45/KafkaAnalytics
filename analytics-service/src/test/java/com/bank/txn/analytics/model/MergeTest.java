package com.bank.txn.analytics.model;

import com.bank.txn.analytics.stream.EnrichedEvent;
import com.bank.txn.analytics.stream.Lateness;
import com.bank.txn.common.model.*;
import com.bank.txn.common.model.Currency;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Per-partition partial aggregates must merge to exactly the same result as one global aggregate. */
class MergeTest {

    private static EnrichedEvent ev(String amount, TransactionStatus status, TransactionType type, Lateness l, boolean ooo) {
        TransactionEvent e = new TransactionEvent(UUID.randomUUID().toString(), "A", 1, type, Channel.ATM, status,
                new BigDecimal(amount), Currency.INR, null, Region.EAST, Instant.EPOCH, Instant.EPOCH);
        return new EnrichedEvent(e, false, ooo, false, l);
    }

    @Test
    void mergedPartialsEqualSingleAggregate() {
        EnrichedEvent[] events = {
                ev("10.00", TransactionStatus.APPROVED, TransactionType.DEPOSIT, Lateness.ON_TIME, false),
                ev("250.00", TransactionStatus.DECLINED, TransactionType.PAYMENT, Lateness.LATE_ACCEPTED, true),
                ev("99.50", TransactionStatus.APPROVED, TransactionType.PAYMENT, Lateness.ON_TIME, false),
                ev("5000.00", TransactionStatus.APPROVED, TransactionType.TRANSFER, Lateness.LATE_ACCEPTED, false)};
        WindowAgg global = new WindowAgg();
        WindowAgg p0 = new WindowAgg();
        WindowAgg p1 = new WindowAgg();
        for (int i = 0; i < events.length; i++) {
            global.add(events[i]);
            (i % 2 == 0 ? p0 : p1).add(events[i]);
        }
        WindowAgg merged = new WindowAgg().merge(p0).merge(p1);
        assertThat(merged).usingRecursiveComparison().withComparatorForType(BigDecimal::compareTo, BigDecimal.class)
                .isEqualTo(global);
    }

    @Test
    void totalsMerge() {
        PipelineTotals a = new PipelineTotals();
        a.received = 10; a.processed = 8; a.duplicates = 1; a.lateDropped = 1; a.maxEventTime = 5;
        PipelineTotals b = new PipelineTotals();
        b.received = 3; b.processed = 3; b.outOfOrder = 2; b.gaps = 1; b.lateAccepted = 1; b.maxEventTime = 9;
        PipelineTotals m = new PipelineTotals().merge(a).merge(b);
        assertThat(m.received).isEqualTo(13);
        assertThat(m.processed).isEqualTo(11);
        assertThat(m.outOfOrder).isEqualTo(2);
        assertThat(m.lateAccepted + m.lateDropped).isEqualTo(2);
        assertThat(m.maxEventTime).isEqualTo(9);
    }
}
