package com.bank.txn.producer.generator;

/** Ground-truth counters of what the generator produced and injected. */
public record GeneratorStats(
        long generated,
        long sent,
        long acked,
        long sendErrors,
        long pending,
        long injectedLate,
        long injectedOutOfOrder,
        long injectedDuplicates,
        long injectedHighValue,
        long injectedBurstEvents) {
}
