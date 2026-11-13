package com.bank.txn.producer.generator;

/** What kind of irregularity (if any) the generator injected for a planned event. */
public enum Injection { NONE, LATE, OUT_OF_ORDER, DUPLICATE, HIGH_VALUE, BURST }
