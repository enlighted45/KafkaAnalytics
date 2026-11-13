package com.bank.txn.producer.api;

import com.bank.txn.producer.generator.GeneratorService;
import com.bank.txn.producer.generator.GeneratorSettings;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

/** Control the transaction generator at runtime (used by the dashboard). */
@RestController
@RequestMapping("/api/generator")
public class GeneratorController {
    private final GeneratorService service;

    public GeneratorController(GeneratorService service) {
        this.service = service;
    }

    @GetMapping
    public GeneratorStatus status() {
        return new GeneratorStatus(service.isRunning(), service.settings(), service.stats());
    }

    @PostMapping("/start")
    public GeneratorStatus start() {
        service.start();
        return status();
    }

    @PostMapping("/stop")
    public GeneratorStatus stop() {
        service.stop();
        return status();
    }

    @PutMapping("/settings")
    public GeneratorStatus update(@Valid @RequestBody GeneratorSettings settings) {
        service.update(settings);
        return status();
    }
}
