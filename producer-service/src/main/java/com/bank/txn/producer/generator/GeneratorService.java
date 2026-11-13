package com.bank.txn.producer.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.random.RandomGenerator;

/**
 * Drives the generator: every tick (100 ms) it generates the right number of events for the configured rate,
 * holds back delayed (late / swapped / duplicate) events in a priority queue and sends everything that is due.
 */
public class GeneratorService {
    public static final long TICK_MS = 100;
    private static final Logger log = LoggerFactory.getLogger(GeneratorService.class);

    private final TransactionGenerator generator;
    private final EventSender sender;
    private final Clock clock;
    private final RandomGenerator random;
    private final AtomicReference<GeneratorSettings> settings;
    private final AtomicBoolean running = new AtomicBoolean(false);
    /** Ordered by send time, then FIFO, so events due at the same instant keep their generation order. */
    private record Queued(PlannedEvent planned, long order) {
    }

    private final PriorityQueue<Queued> queue = new PriorityQueue<>(
            Comparator.comparing((Queued q) -> q.planned().sendAt()).thenComparingLong(Queued::order));
    private long order;
    private double carry;

    private final AtomicLong generated = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final Map<Injection, AtomicLong> injected = new EnumMap<>(Injection.class);

    public GeneratorService(TransactionGenerator generator, EventSender sender, Clock clock, RandomGenerator random,
                            GeneratorSettings initial) {
        this.generator = generator;
        this.sender = sender;
        this.clock = clock;
        this.random = random;
        this.settings = new AtomicReference<>(initial);
        for (Injection i : Injection.values()) {
            injected.put(i, new AtomicLong());
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Generator started with {}", settings.get());
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Generator stopped (delayed events still in queue will be delivered)");
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public GeneratorSettings settings() {
        return settings.get();
    }

    public void update(GeneratorSettings newSettings) {
        settings.set(newSettings);
        log.info("Generator settings updated: {}", newSettings);
    }

    /** One scheduler tick. Package-visible state changes are synchronized on the queue. */
    public void tick() {
        Instant now = clock.instant();
        GeneratorSettings s = settings.get();
        synchronized (queue) {
            if (running.get()) {
                double want = s.ratePerSecond() * TICK_MS / 1000.0 + carry;
                int count = (int) want;
                carry = want - count;
                plan(generator.generate(count, now, s));
                if (random.nextDouble() < s.burstChancePerSecond() * TICK_MS / 1000.0) {
                    plan(generator.burst(now, s));
                }
            }
            // delayed events are always delivered, even after stop
            while (!queue.isEmpty() && !queue.peek().planned().sendAt().isAfter(now)) {
                send(queue.poll().planned());
            }
        }
    }

    private void plan(List<PlannedEvent> events) {
        for (PlannedEvent p : events) {
            if (p.injection() != Injection.DUPLICATE) {
                generated.incrementAndGet();
            }
            injected.get(p.injection()).incrementAndGet();
            queue.add(new Queued(p, order++));
        }
    }

    private void send(PlannedEvent p) {
        sent.incrementAndGet();
        try {
            sender.send(p.event().withProducedAt(clock.instant())).whenComplete((r, ex) -> {
                if (ex == null) {
                    acked.incrementAndGet();
                } else {
                    errors.incrementAndGet();
                    log.warn("Send failed for {}: {}", p.event().eventId(), ex.toString());
                }
            });
        } catch (RuntimeException ex) {
            errors.incrementAndGet();
            log.warn("Send rejected for {}: {}", p.event().eventId(), ex.toString());
        }
    }

    public GeneratorStats stats() {
        int pending;
        synchronized (queue) {
            pending = queue.size();
        }
        return new GeneratorStats(generated.get(), sent.get(), acked.get(), errors.get(), pending,
                injected.get(Injection.LATE).get(), injected.get(Injection.OUT_OF_ORDER).get(),
                injected.get(Injection.DUPLICATE).get(), injected.get(Injection.HIGH_VALUE).get(),
                injected.get(Injection.BURST).get());
    }
}
