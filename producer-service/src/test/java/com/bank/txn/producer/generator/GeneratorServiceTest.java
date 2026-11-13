package com.bank.txn.producer.generator;

import com.bank.txn.common.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class GeneratorServiceTest {

    /** Clock whose time the test moves forward. */
    static class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-12T10:00:00Z");
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
        void advanceMs(long ms) { now = now.plusMillis(ms); }
    }

    private final MutableClock clock = new MutableClock();
    private final List<TransactionEvent> sent = new ArrayList<>();
    private final EventSender sender = e -> { sent.add(e); return CompletableFuture.completedFuture(null); };

    private GeneratorService service(GeneratorSettings s) {
        Random r = new Random(7);
        return new GeneratorService(new TransactionGenerator(r), sender, clock, r, s);
    }

    private void runSeconds(GeneratorService svc, int seconds) {
        for (int i = 0; i < seconds * 10; i++) {
            svc.tick();
            clock.advanceMs(GeneratorService.TICK_MS);
        }
    }

    @Test
    void sendsConfiguredRateIncludingFractionalRates() {
        GeneratorService svc = service(new GeneratorSettings(37, 1000, 0, 90, 0, 0, 0, 0, 12));
        svc.start();
        runSeconds(svc, 10);
        assertThat(sent).hasSizeBetween(369, 371);
        assertThat(svc.stats().acked()).isEqualTo(sent.size());
    }

    @Test
    void doesNothingWhenStopped() {
        GeneratorService svc = service(GeneratorSettings.defaults());
        runSeconds(svc, 3);
        assertThat(sent).isEmpty();
    }

    @Test
    void lateEventsAreHeldBackAndStillDeliveredAfterStop() {
        GeneratorService svc = service(new GeneratorSettings(100, 1000, 1.0, 20, 0, 0, 0, 0, 12));
        svc.start();
        runSeconds(svc, 1);
        svc.stop();
        assertThat(sent).isEmpty();
        assertThat(svc.stats().pending()).isEqualTo(100);
        runSeconds(svc, 21);
        assertThat(sent).hasSize(100);
        assertThat(sent).allSatisfy(e -> assertThat(e.producedAt()).isAfter(e.eventTime().plusSeconds(4)));
    }

    @Test
    void statsCountInjections() {
        GeneratorService svc = service(new GeneratorSettings(1000, 1000, 0.1, 10, 0.1, 0.1, 0.1, 0, 12));
        svc.start();
        runSeconds(svc, 5);
        svc.stop();
        runSeconds(svc, 11);
        GeneratorStats st = svc.stats();
        assertThat(st.generated()).isEqualTo(5000 + st.injectedOutOfOrder()); // a swapped pair = 2 transactions
        assertThat(st.sent()).isEqualTo(st.generated() + st.injectedDuplicates());
        assertThat(st.pending()).isZero();
        assertThat(st.injectedLate()).isBetween(400L, 600L);
    }

    @Test
    void sendFailuresAreCounted() {
        Random r = new Random(1);
        GeneratorService svc = new GeneratorService(new TransactionGenerator(r),
                e -> CompletableFuture.failedFuture(new RuntimeException("broker down")), clock, r,
                new GeneratorSettings(10, 100, 0, 90, 0, 0, 0, 0, 12));
        svc.start();
        runSeconds(svc, 1);
        assertThat(svc.stats().sendErrors()).isEqualTo(10);
    }

    @Test
    void eventsDueAtTheSameTimeKeepGenerationOrder() {
        GeneratorService svc = service(new GeneratorSettings(2000, 3, 0, 90, 0, 0, 0, 0, 12));
        svc.start();
        runSeconds(svc, 2);
        java.util.Map<String, Long> last = new java.util.HashMap<>();
        for (TransactionEvent e : sent) {
            Long prev = last.put(e.accountId(), e.sequenceNo());
            if (prev != null) {
                assertThat(e.sequenceNo()).isEqualTo(prev + 1);
            }
        }
    }
}
