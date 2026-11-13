package com.bank.txn.producer;

import com.bank.txn.common.model.TransactionEvent;
import com.bank.txn.producer.generator.GeneratorService;
import com.bank.txn.producer.generator.GeneratorSettings;
import com.bank.txn.producer.generator.TransactionGenerator;
import com.bank.txn.producer.kafka.KafkaEventSender;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.util.Random;

@Configuration
public class GeneratorConfig {

    @Bean
    GeneratorService generatorService(KafkaTemplate<String, TransactionEvent> template, GeneratorSettings settings) {
        Random random = new Random();
        return new GeneratorService(new TransactionGenerator(random, (System.currentTimeMillis() / 1000) * 1_000_000L), new KafkaEventSender(template),
                Clock.systemUTC(), random, settings);
    }

    @Bean
    GeneratorRunner generatorRunner(GeneratorService service, @Value("${generator-autostart:true}") boolean autostart) {
        return new GeneratorRunner(service, autostart);
    }

    static class GeneratorRunner {
        private final GeneratorService service;
        private final boolean autostart;

        GeneratorRunner(GeneratorService service, boolean autostart) {
            this.service = service;
            this.autostart = autostart;
        }

        @EventListener(ApplicationReadyEvent.class)
        void onReady() {
            if (autostart) {
                service.start();
            }
        }

        @Scheduled(fixedRate = GeneratorService.TICK_MS)
        void tick() {
            service.tick();
        }
    }
}
