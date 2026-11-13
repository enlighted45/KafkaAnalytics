package com.bank.txn.analytics.dashboard;

import java.math.BigDecimal;

/**
 * Headline numbers.
 *
 * @param tps            transactions/s in the latest complete window
 * @param volumeLastMin  approved volume (INR) over the last minute of complete windows
 * @param countLastMin   transactions over the last minute of complete windows
 * @param avgTicket      average approved amount over the last minute
 * @param approvalRate   approved / all over the last minute (0..1)
 * @param peakTps        highest window TPS in the chart history
 */
public record Kpis(double tps, BigDecimal volumeLastMin, long countLastMin, BigDecimal avgTicket,
                   double approvalRate, double peakTps) {
    public static final Kpis EMPTY = new Kpis(0, BigDecimal.ZERO, 0, BigDecimal.ZERO, 0, 0);
}
