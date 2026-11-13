package com.bank.txn.producer.api;

import com.bank.txn.producer.generator.GeneratorSettings;
import com.bank.txn.producer.generator.GeneratorStats;

public record GeneratorStatus(boolean running, GeneratorSettings settings, GeneratorStats stats) {
}
