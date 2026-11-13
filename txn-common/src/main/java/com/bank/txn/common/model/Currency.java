package com.bank.txn.common.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Supported currencies with a fixed reference rate to the reporting currency (INR). */
public enum Currency {
    INR("1"), USD("83.50"), EUR("90.20"), GBP("105.80");

    private final BigDecimal rateToInr;

    Currency(String rateToInr) {
        this.rateToInr = new BigDecimal(rateToInr);
    }

    public BigDecimal toInr(BigDecimal amount) {
        return amount.multiply(rateToInr).setScale(2, RoundingMode.HALF_UP);
    }
}
