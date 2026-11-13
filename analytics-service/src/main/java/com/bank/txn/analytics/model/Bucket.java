package com.bank.txn.analytics.model;

import java.math.BigDecimal;

/** Count + volume for one category (e.g. one transaction type). */
public class Bucket {
    public long count;
    public BigDecimal volume = BigDecimal.ZERO;

    public Bucket() {
    }

    public Bucket(long count, BigDecimal volume) {
        this.count = count;
        this.volume = volume;
    }

    void merge(Bucket other) {
        count += other.count;
        volume = volume.add(other.volume);
    }

    void add(BigDecimal amount) {
        count++;
        volume = volume.add(amount);
    }
}
