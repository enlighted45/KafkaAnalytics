package com.bank.txn.analytics.dashboard;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;

/** Pure KPI computation from a list of windows. */
public final class KpiCalculator {
    private KpiCalculator() {
    }

    public static Kpis compute(List<WindowPoint> windows, long windowMs, long nowMs) {
        List<WindowPoint> complete = windows.stream().filter(w -> w.end() <= nowMs)
                .sorted(Comparator.comparingLong(WindowPoint::start)).toList();
        if (complete.isEmpty()) {
            return Kpis.EMPTY;
        }
        double seconds = windowMs / 1000.0;
        WindowPoint latest = complete.getLast();
        double tps = latest.agg().count / seconds;
        long lastMinStart = latest.end() - 60_000;
        long count = 0, approved = 0;
        BigDecimal volume = BigDecimal.ZERO;
        for (WindowPoint w : complete) {
            if (w.start() >= lastMinStart) {
                count += w.agg().count;
                approved += w.agg().approved;
                volume = volume.add(w.agg().volume);
            }
        }
        double peak = complete.stream().mapToLong(w -> w.agg().count).max().orElse(0) / seconds;
        BigDecimal avg = approved == 0 ? BigDecimal.ZERO : volume.divide(BigDecimal.valueOf(approved), 2, RoundingMode.HALF_UP);
        double rate = count == 0 ? 0 : (double) approved / count;
        return new Kpis(round1(tps), volume.setScale(2, RoundingMode.HALF_UP), count, avg, rate, round1(peak));
    }

    private static double round1(double v) {
        return Math.round(v * 10) / 10.0;
    }
}
