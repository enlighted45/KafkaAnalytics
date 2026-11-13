package com.bank.txn.producer.generator;

import com.bank.txn.common.model.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.random.RandomGenerator;

/**
 * Pure (Kafka-free) generator of realistic banking transactions.
 * <p>
 * Every account has its own increasing {@code sequenceNo}. Irregularities are injected on purpose so the
 * stream processor has something to handle:
 * <ul>
 *   <li>LATE: event time is "now" but delivery is delayed 5s..maxLateness (like an offline ATM syncing later)</li>
 *   <li>OUT_OF_ORDER: two consecutive events of one account are delivered in swapped order</li>
 *   <li>DUPLICATE: the same event (same eventId) is delivered twice (like a client retry)</li>
 *   <li>HIGH_VALUE: unusually large amount</li>
 *   <li>BURST: one account makes many transactions within a few seconds</li>
 * </ul>
 */
public class TransactionGenerator {

    static final BigDecimal HIGH_VALUE_MIN_INR = new BigDecimal("200000");
    private static final String[] MERCHANT_CATEGORIES =
            {"GROCERY", "FUEL", "TRAVEL", "DINING", "ELECTRONICS", "UTILITIES", "HEALTHCARE", "ENTERTAINMENT", "RETAIL"};

    private final RandomGenerator random;
    private final ConcurrentHashMap<String, Long> sequences = new ConcurrentHashMap<>();
    private final long sequenceBase;

    /** Test constructor: sequences start at 1. */
    public TransactionGenerator(RandomGenerator random) {
        this(random, 0);
    }

    /**
     * @param sequenceBase first sequence number minus one for every account. Production uses a value derived from
     *                     the startup time so sequences keep increasing across generator restarts.
     */
    public TransactionGenerator(RandomGenerator random, long sequenceBase) {
        this.random = random;
        this.sequenceBase = sequenceBase;
    }

    /** Generates {@code count} logical transactions (plus any injected duplicates / swapped pairs). */
    public List<PlannedEvent> generate(int count, Instant now, GeneratorSettings s) {
        List<PlannedEvent> out = new ArrayList<>(count + 4);
        for (int i = 0; i < count; i++) {
            String account = accountId(random.nextInt(s.accounts()));
            double r = random.nextDouble();
            if (r < s.lateRatio()) {
                long delayMs = randomBetween(GeneratorSettings.MIN_LATENESS_SECONDS * 1000L, s.maxLatenessSeconds() * 1000L);
                out.add(new PlannedEvent(newEvent(account, now, false), now.plusMillis(delayMs), Injection.LATE));
            } else if ((r -= s.lateRatio()) < s.outOfOrderRatio()) {
                TransactionEvent first = newEvent(account, now, false);
                TransactionEvent second = newEvent(account, now.plusMillis(200), false);
                out.add(new PlannedEvent(second, now, Injection.NONE));
                out.add(new PlannedEvent(first, now.plusMillis(500), Injection.OUT_OF_ORDER));
            } else if ((r -= s.outOfOrderRatio()) < s.duplicateRatio()) {
                TransactionEvent e = newEvent(account, now, false);
                out.add(new PlannedEvent(e, now, Injection.NONE));
                out.add(new PlannedEvent(e, now.plusMillis(randomBetween(100, 2000)), Injection.DUPLICATE));
            } else if ((r -= s.duplicateRatio()) < s.highValueRatio()) {
                out.add(new PlannedEvent(newEvent(account, now, true), now, Injection.HIGH_VALUE));
            } else {
                out.add(new PlannedEvent(newEvent(account, now, false), now, Injection.NONE));
            }
        }
        return out;
    }

    /** One account firing {@code size} transactions spread over ~3 seconds. */
    public List<PlannedEvent> burst(Instant now, GeneratorSettings s) {
        String account = accountId(random.nextInt(s.accounts()));
        List<PlannedEvent> out = new ArrayList<>(s.burstSize());
        for (int i = 0; i < s.burstSize(); i++) {
            Instant t = now.plusMillis(i * 250L);
            out.add(new PlannedEvent(newEvent(account, t, false), t, Injection.BURST));
        }
        return out;
    }

    static String accountId(int index) {
        return "ACC-%06d".formatted(index);
    }

    TransactionEvent newEvent(String accountId, Instant eventTime, boolean highValue) {
        long seq = sequences.merge(accountId, sequenceBase + 1, (old, one) -> old + 1);
        TransactionType type = pick(TransactionType.values(), 0.20, 0.25, 0.20, 0.35);
        Channel channel = switch (type) {
            case WITHDRAWAL -> pick(new Channel[]{Channel.ATM, Channel.BRANCH}, 0.85, 0.15);
            case DEPOSIT -> pick(new Channel[]{Channel.BRANCH, Channel.ATM, Channel.ONLINE, Channel.MOBILE}, 0.3, 0.2, 0.2, 0.3);
            case TRANSFER -> pick(new Channel[]{Channel.ONLINE, Channel.MOBILE, Channel.BRANCH}, 0.4, 0.5, 0.1);
            case PAYMENT -> pick(new Channel[]{Channel.POS, Channel.ONLINE, Channel.MOBILE}, 0.5, 0.25, 0.25);
        };
        Currency currency = pick(Currency.values(), 0.85, 0.07, 0.05, 0.03);
        BigDecimal amountInr = highValue
                ? randomAmount(HIGH_VALUE_MIN_INR.doubleValue(), 1_500_000)
                : typicalAmountInr(type);
        // express the amount in the transaction currency
        BigDecimal amount = amountInr.divide(currency.toInr(BigDecimal.ONE), 2, RoundingMode.HALF_UP);
        if (amount.signum() <= 0) {
            amount = new BigDecimal("1.00");
        }
        double declineChance = highValue ? 0.25 : 0.03;
        TransactionStatus status = random.nextDouble() < declineChance ? TransactionStatus.DECLINED : TransactionStatus.APPROVED;
        Region region = Region.values()[Math.floorMod(accountId.hashCode(), Region.values().length)];
        return new TransactionEvent(UUID.randomUUID().toString(), accountId, seq, type, channel, status, amount, currency,
                type == TransactionType.PAYMENT ? MERCHANT_CATEGORIES[random.nextInt(MERCHANT_CATEGORIES.length)] : null,
                region, eventTime, null);
    }

    private BigDecimal typicalAmountInr(TransactionType type) {
        // log-normal-ish: many small, a few big
        double median = switch (type) {
            case DEPOSIT -> 15_000;
            case WITHDRAWAL -> 4_000;
            case TRANSFER -> 12_000;
            case PAYMENT -> 1_500;
        };
        double v = median * Math.exp(random.nextGaussian() * 0.9);
        return BigDecimal.valueOf(Math.min(Math.max(v, 10), 150_000)).setScale(2, RoundingMode.HALF_UP);
    }

    private BigDecimal randomAmount(double min, double max) {
        return BigDecimal.valueOf(min + random.nextDouble() * (max - min)).setScale(2, RoundingMode.HALF_UP);
    }

    private long randomBetween(long min, long max) {
        return min + (long) (random.nextDouble() * (max - min));
    }

    @SafeVarargs
    private <T> T pick(T[] values, double... weights) {
        double r = random.nextDouble();
        for (int i = 0; i < values.length; i++) {
            r -= weights[i];
            if (r < 0) {
                return values[i];
            }
        }
        return values[values.length - 1];
    }

}
