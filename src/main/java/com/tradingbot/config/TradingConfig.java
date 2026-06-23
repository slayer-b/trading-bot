package com.tradingbot.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradingbot.bot.SimulationOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Provides shared infrastructure beans.
 * All bot-specific wiring is handled by {@link SimulationOrchestrator}.
 */
@Configuration
public class TradingConfig {

    private static final Logger log = LoggerFactory.getLogger(TradingConfig.class);

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean
    public WebClient webClient() {
        return WebClient.builder().build();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady(ApplicationReadyEvent event) {
        log.info("[Config] Application ready — handing off to SimulationOrchestrator");
        event.getApplicationContext()
             .getBean(SimulationOrchestrator.class)
             .run();
    }
}
