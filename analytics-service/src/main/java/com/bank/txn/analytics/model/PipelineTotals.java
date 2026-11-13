package com.bank.txn.analytics.model;

import com.bank.txn.analytics.stream.EnrichedEvent;

/** Running totals since the pipeline started, per input partition (persisted in a state store, survives restarts). */
public class PipelineTotals {
    public long received;
    public long processed;
    public long duplicates;
    public long outOfOrder;
    public long gaps;
    public long lateAccepted;
    public long lateDropped;
    public long maxEventTime;

    public PipelineTotals merge(PipelineTotals o) {
        received += o.received;
        processed += o.processed;
        duplicates += o.duplicates;
        outOfOrder += o.outOfOrder;
        gaps += o.gaps;
        lateAccepted += o.lateAccepted;
        lateDropped += o.lateDropped;
        maxEventTime = Math.max(maxEventTime, o.maxEventTime);
        return this;
    }

    public PipelineTotals add(EnrichedEvent e) {
        received++;
        if (e.duplicate()) {
            duplicates++;
            return this;
        }
        if (e.outOfOrder()) outOfOrder++;
        if (e.gap()) gaps++;
        switch (e.lateness()) {
            case LATE_ACCEPTED -> lateAccepted++;
            case LATE_DROPPED -> lateDropped++;
            default -> { }
        }
        if (e.accepted()) processed++;
        maxEventTime = Math.max(maxEventTime, e.event().eventTime().toEpochMilli());
        return this;
    }
}
