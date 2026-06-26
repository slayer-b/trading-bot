package com.tradingbot.service.impl;

import com.tradingbot.model.Candle;
import com.tradingbot.service.HistoryService;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class OkxHistoryService implements HistoryService {

    private final WebClient webClient = WebClient.builder().baseUrl("https://okx.com").build();

    @Override
    public String marketName() {
        return "okx";
    }

    @Override
    public Flux<Candle> fetchHistory(String symbol, int days) {
        long afterMs = LocalDateTime.now().minusDays(days).toInstant(ZoneOffset.UTC).toEpochMilli();
        String okxSymbol = symbol.contains("-") ? symbol : symbol.replace("USDT", "-USDT").replace("USDC", "-USDC");

        System.out.println("[HISTORY SERVICE] Requesting historical OKX candles for symbol: " + symbol);

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v5/market/history-candles")
                        .queryParam("instId", okxSymbol)
                        .queryParam("bar", "5m")
                        .queryParam("after", afterMs)
                        .queryParam("limit", "300")
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .flatMapMany(response -> {
                    if (!"0".equals(response.get("code"))) {
                        System.err.println("[HISTORY SERVICE ERROR] OKX API error: " + response.get("msg"));
                        return Flux.empty();
                    }
                    List<List<String>> data = (List<List<String>>) response.getOrDefault("data", Collections.emptyList());

                    return Flux.fromIterable(data)
                            .map(raw -> {
                                LocalDateTime openTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(raw.get(0))), ZoneOffset.UTC);
                                return Candle.builder()
                                        .symbol(symbol)
                                        .openTime(openTime)
                                        .closeTime(openTime.plusMinutes(5))
                                        .open(new BigDecimal(raw.get(1)))
                                        .high(new BigDecimal(raw.get(2)))
                                        .low(new BigDecimal(raw.get(3)))
                                        .close(new BigDecimal(raw.get(4)))
                                        .volume(new BigDecimal(raw.get(5)))
                                        .timeframe(Duration.ofMinutes(5))
                                        .build();
                            })
                            .sort((c1, c2) -> c1.openTime().compareTo(c2.openTime()));
                })
                .onErrorResume(e -> {
                    System.err.println("[HISTORY SERVICE ERROR] Connection failure for " + symbol + ": " + e.getMessage());
                    return Flux.empty();
                });
    }
}
