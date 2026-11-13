package com.bank.txn.analytics.dashboard;

import com.bank.txn.analytics.model.WindowAgg;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KpiCalculatorTest {
    static final long W = 10_000;

    static WindowPoint w(long start, long count, long approved, String volume) {
        WindowAgg a = new WindowAgg();
        a.count = count;
        a.approved = approved;
        a.declined = count - approved;
        a.volume = new BigDecimal(volume);
        return new WindowPoint(start, start + W, true, a);
    }

    @Test
    void emptyInputGivesZeros() {
        assertThat(KpiCalculator.compute(List.of(), W, 100_000)).isEqualTo(Kpis.EMPTY);
    }

    @Test
    void tpsUsesLatestCompleteWindowAndIgnoresTheOpenOne() {
        List<WindowPoint> ws = List.of(w(0, 100, 100, "1000"), w(10_000, 500, 500, "5000"), w(20_000, 7, 7, "70"));
        // now = 25s: window [20s,30s) is still open
        Kpis k = KpiCalculator.compute(ws, W, 25_000);
        assertThat(k.tps()).isEqualTo(50.0);
        assertThat(k.peakTps()).isEqualTo(50.0);
    }

    @Test
    void lastMinuteSumsOnlyTheSixMostRecentCompleteWindows() {
        List<WindowPoint> ws = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ws.add(w(i * W, 10, 9, "900"));
        }
        Kpis k = KpiCalculator.compute(ws, W, 100_000);
        assertThat(k.countLastMin()).isEqualTo(60);
        assertThat(k.volumeLastMin()).isEqualByComparingTo("5400");
        assertThat(k.avgTicket()).isEqualByComparingTo("100.00");
        assertThat(k.approvalRate()).isEqualTo(0.9);
    }
}
