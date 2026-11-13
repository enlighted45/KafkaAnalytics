package com.bank.txn.analytics.model;

import com.bank.txn.analytics.stream.EnrichedEvent;
import com.bank.txn.analytics.stream.Lateness;
import com.bank.txn.common.model.TransactionEvent;

import java.math.BigDecimal;
import java.util.Map;
import java.util.TreeMap;

/** Aggregate of all accepted transactions in one event-time window. All amounts in INR. */
public class WindowAgg {
    public long count;
    public long approved;
    public long declined;
    public BigDecimal volume = BigDecimal.ZERO;          // approved volume
    public BigDecimal maxAmount = BigDecimal.ZERO;
    public long lateAccepted;
    public long outOfOrder;
    public Map<String, Bucket> byType = new TreeMap<>();
    public Map<String, Bucket> byChannel = new TreeMap<>();
    public Map<String, Bucket> byRegion = new TreeMap<>();
    public Map<String, Long> byCurrency = new TreeMap<>();

    /** Adds another partial aggregate (e.g. the same window from another partition) into this one. */
    public WindowAgg merge(WindowAgg o) {
        count += o.count;
        approved += o.approved;
        declined += o.declined;
        volume = volume.add(o.volume);
        if (o.maxAmount.compareTo(maxAmount) > 0) {
            maxAmount = o.maxAmount;
        }
        lateAccepted += o.lateAccepted;
        outOfOrder += o.outOfOrder;
        mergeBuckets(byType, o.byType);
        mergeBuckets(byChannel, o.byChannel);
        mergeBuckets(byRegion, o.byRegion);
        o.byCurrency.forEach((k, v) -> byCurrency.merge(k, v, Long::sum));
        return this;
    }

    private static void mergeBuckets(Map<String, Bucket> into, Map<String, Bucket> from) {
        from.forEach((k, b) -> into.computeIfAbsent(k, x -> new Bucket()).merge(b));
    }

    public WindowAgg add(EnrichedEvent enriched) {
        TransactionEvent e = enriched.event();
        BigDecimal inr = e.amountInr();
        count++;
        if (e.approved()) {
            approved++;
            volume = volume.add(inr);
            byType.computeIfAbsent(e.type().name(), k -> new Bucket()).add(inr);
            byChannel.computeIfAbsent(e.channel().name(), k -> new Bucket()).add(inr);
            byRegion.computeIfAbsent(e.region() == null ? "UNKNOWN" : e.region().name(), k -> new Bucket()).add(inr);
        } else {
            declined++;
        }
        byCurrency.merge(e.currency().name(), 1L, Long::sum);
        if (inr.compareTo(maxAmount) > 0) {
            maxAmount = inr;
        }
        if (enriched.lateness() == Lateness.LATE_ACCEPTED) {
            lateAccepted++;
        }
        if (enriched.outOfOrder()) {
            outOfOrder++;
        }
        return this;
    }
}
