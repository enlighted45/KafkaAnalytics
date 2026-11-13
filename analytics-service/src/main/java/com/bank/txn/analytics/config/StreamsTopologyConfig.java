package com.bank.txn.analytics.config;

import com.bank.txn.analytics.stream.LatencyTracker;
import com.bank.txn.analytics.stream.TransactionTopology;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.time.Clock;

@Configuration
@EnableKafkaStreams
public class StreamsTopologyConfig {
    private static final Logger log = LoggerFactory.getLogger(StreamsTopologyConfig.class);

    @Bean
    LatencyTracker latencyTracker() {
        return new LatencyTracker(4096, Clock.systemUTC());
    }

    /** Registers the analytics topology on Spring's StreamsBuilder. */
    @Bean
    Object transactionTopology(StreamsBuilder builder, AnalyticsProperties props, LatencyTracker latency) {
        TransactionTopology.build(builder, props.topologyConfig(), latency);
        return new Object();
    }

    /** A crashed stream thread is replaced instead of shutting the whole application down. */
    @Bean
    StreamsBuilderFactoryBeanConfigurer streamsResilience() {
        return fb -> {
            fb.setStreamsUncaughtExceptionHandler(ex -> {
                log.error("Stream thread failed, replacing it", ex);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            fb.setStateListener((newState, oldState) -> log.info("Kafka Streams state {} -> {}", oldState, newState));
        };
    }
}
