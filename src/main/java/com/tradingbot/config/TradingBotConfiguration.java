package com.tradingbot.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.core.TradingEngine2;
import com.tradingbot.service.HistoricalCandleRepository;
import com.tradingbot.service.RealtimeCandleSource;
import com.tradingbot.service.impl.OkxHistoricalCandleRepository;
import com.tradingbot.service.impl.OkxRealtimeCandleSource;
import com.tradingbot.strategy.BreakoutRetestStrategy;
import com.tradingbot.strategy.TradingStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;

@Configuration
public class TradingBotConfiguration {

    /**
     * Создает базовый WebClient.Builder, который будет использоваться 
     * для выполнения REST-запросов к бирже.
     */
    @Bean
    public WebClient.Builder okxWebClientBuilder(@Value("${trading.okx.rest-url:https://okx.com}") String restUrl) {
        return WebClient.builder()
                .baseUrl(restUrl);
    }

    /**
     * Создает репозиторий для загрузки исторических 5м свечей через REST API OKX.
     */
    @Bean
    public HistoricalCandleRepository historicalCandleRepository(WebClient.Builder okxWebClientBuilder) {
        return new OkxHistoricalCandleRepository(okxWebClientBuilder);
    }

    /**
     * Создает источник живого потока свечей через WebSockets OKX.
     * Автоматически внедряет Jackson ObjectMapper, настроенный внутри Spring Boot.
     */
    @Bean
    public RealtimeCandleSource realtimeCandleSource(ObjectMapper objectMapper) {
        return new OkxRealtimeCandleSource(objectMapper);
    }

    /**
     * Собирает финальный торговый движок TradingEngine2, связывая вместе 
     * стратегию, исторические данные и вебсокет.
     */
//    @Bean
//    public TradingEngine2 tradingEngine2(
//            TradingStrategy breakoutRetestStrategy,
//            HistoricalCandleRepository historicalCandleRepository,
//            RealtimeCandleSource realtimeCandleSource) {
//
//        return new TradingEngine2(
//                breakoutRetestStrategy,
//                historicalCandleRepository,
//                realtimeCandleSource
//        );
//    }
}
