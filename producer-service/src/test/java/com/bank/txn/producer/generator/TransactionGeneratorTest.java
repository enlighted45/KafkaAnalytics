package com.bank.txn.producer.generator;

import com.bank.txn.common.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class TransactionGeneratorTest {

    private static final Instant NOW = Instant.parse("2026-11-12T10:00:00Z");
    private final TransactionGenerator generator = new TransactionGenerator(new Random(42));

    private static GeneratorSettings settings(double late, double ooo, double dup, double high) {
        return new GeneratorSettings(100, 50, late, 90, ooo, dup, high, 0, 12);
    }

    @Test
    void sequenceNumbersIncreaseByOnePerAccountInGenerationOrder() {
        List<PlannedEvent> events = generator.generate(5000, NOW, settings(0, 0, 0, 0));
        Map<String, List<Long>> byAccount = events.stream().map(PlannedEvent::event)
                .collect(Collectors.groupingBy(TransactionEvent::accountId, LinkedHashMap::new,
                        Collectors.mapping(TransactionEvent::sequenceNo, Collectors.toList())));
        byAccount.forEach((acc, seqs) -> {
            for (int i = 0; i < seqs.size(); i++) {
                assertThat(seqs.get(i)).as(acc).isEqualTo(i + 1L);
            }
        });
    }

    @Test
    void regularEventsAreValid() {
        for (PlannedEvent p : generator.generate(2000, NOW, settings(0, 0, 0, 0))) {
            TransactionEvent e = p.event();
            assertThat(p.sendAt()).isEqualTo(NOW);
            assertThat(e.eventId()).isNotBlank();
            assertThat(e.accountId()).matches("ACC-\\d{6}");
            assertThat(e.amount()).isPositive();
            assertThat(e.amount().scale()).isEqualTo(2);
            assertThat(e.amountInr()).isLessThan(new BigDecimal("160000"));
            assertThat(e.eventTime()).isEqualTo(NOW);
        }
    }

    @Test
    void injectionRatiosMatchSettings() {
        int n = 40_000;
        List<PlannedEvent> events = generator.generate(n, NOW, settings(0.05, 0.03, 0.02, 0.01));
        Map<Injection, Long> counts = events.stream()
                .collect(Collectors.groupingBy(PlannedEvent::injection, Collectors.counting()));
        assertThat(counts.get(Injection.LATE) / (double) n).isCloseTo(0.05, within(0.006));
        assertThat(counts.get(Injection.OUT_OF_ORDER) / (double) n).isCloseTo(0.03, within(0.005));
        assertThat(counts.get(Injection.DUPLICATE) / (double) n).isCloseTo(0.02, within(0.004));
        assertThat(counts.get(Injection.HIGH_VALUE) / (double) n).isCloseTo(0.01, within(0.003));
    }

    @Test
    void lateEventsKeepEventTimeButAreDeliveredFiveToMaxSecondsLater() {
        List<PlannedEvent> late = generator.generate(20_000, NOW, settings(0.2, 0, 0, 0)).stream()
                .filter(p -> p.injection() == Injection.LATE).toList();
        assertThat(late).isNotEmpty();
        for (PlannedEvent p : late) {
            assertThat(p.event().eventTime()).isEqualTo(NOW);
            assertThat(p.sendAt()).isBetween(NOW.plusSeconds(5), NOW.plusSeconds(90));
        }
    }

    @Test
    void outOfOrderPairDeliversHigherSequenceFirst() {
        List<PlannedEvent> events = generator.generate(1, NOW, settings(0, 1.0, 0, 0));
        assertThat(events).hasSize(2);
        PlannedEvent first = events.get(0), second = events.get(1);
        assertThat(first.event().accountId()).isEqualTo(second.event().accountId());
        assertThat(first.event().sequenceNo()).isEqualTo(second.event().sequenceNo() + 1);
        assertThat(first.sendAt()).isBefore(second.sendAt());
        assertThat(second.injection()).isEqualTo(Injection.OUT_OF_ORDER);
    }

    @Test
    void duplicatesShareEventId() {
        List<PlannedEvent> events = generator.generate(1, NOW, settings(0, 0, 1.0, 0));
        assertThat(events).hasSize(2);
        assertThat(events.get(1).event()).isEqualTo(events.get(0).event());
        assertThat(events.get(1).injection()).isEqualTo(Injection.DUPLICATE);
        assertThat(events.get(1).sendAt()).isAfter(events.get(0).sendAt());
    }

    @Test
    void highValueEventsExceedThreshold() {
        List<PlannedEvent> events = generator.generate(200, NOW, settings(0, 0, 0, 1.0));
        assertThat(events).allSatisfy(p ->
                assertThat(p.event().amountInr()).isGreaterThanOrEqualTo(new BigDecimal("199000")));
    }

    @Test
    void burstIsOneAccountWithConsecutiveSequences() {
        List<PlannedEvent> burst = generator.burst(NOW, settings(0, 0, 0, 0));
        assertThat(burst).hasSize(12);
        assertThat(burst.stream().map(p -> p.event().accountId()).distinct()).hasSize(1);
        for (int i = 1; i < burst.size(); i++) {
            assertThat(burst.get(i).event().sequenceNo()).isEqualTo(burst.get(i - 1).event().sequenceNo() + 1);
        }
    }

    @Test
    void sequenceBaseKeepsSequencesIncreasingAcrossRestarts() {
        TransactionGenerator restarted = new TransactionGenerator(new Random(1), 5_000_000L);
        TransactionEvent e = restarted.generate(1, NOW, settings(0, 0, 0, 0)).getFirst().event();
        assertThat(e.sequenceNo()).isEqualTo(5_000_001L);
    }
}
