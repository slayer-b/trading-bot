package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.strategy.helper.MarketStateTracker;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BreakoutRetestStrategy implements TradingStrategy {

    private static final BigDecimal RISK_PERCENTAGE = new BigDecimal("0.02");
    private static final String QUOTE_ASSET = "USDT";

    // Explicit production ready endpoint configuration to eliminate 301 redirects
    private final WebClient webClient = WebClient.builder().baseUrl("https://www.okx.com").build();

    private static int NUM = 0;
    public BreakoutRetestStrategy() {
        int number = -1;
        synchronized (BreakoutRetestStrategy.class) {
            number = NUM++;
        }
        System.out.println("====================================================");
        System.out.println("[OK] BreakoutRetestStrategy initialized cleanly.");
        System.out.println(number);
        System.out.println("====================================================");
    }

    @Override
    public String name() {
        return "breakout_retest_daily_15m_5m";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        System.out.println("[ENGINE] Strategy evaluate pipeline started for instrument: " + symbol);

        MarketStateTracker tracker = new MarketStateTracker();
        var processedTimestamps = ConcurrentHashMap.<LocalDateTime>newKeySet();

        // 1. Asynchronously load past market candles buffer to build boundary state maps
        Flux<Candle> historicalCandles = fetchOkxHistory(symbol, 2);

        // 2. Chronologically stream historical buffer prior to tracking real-time engine items
        Flux<Candle> combinedStream = Flux.concat(historicalCandles, candles)
                .filter(candle -> processedTimestamps.add(candle.openTime()))
                .doOnNext(c -> System.out.println("[STREAM PROCESSING] Replaying candle timestamp: " + c.openTime() + " | Close: " + c.close()));

        // Guard threshold: prevents old processed warm-up signals from executing on live endpoints
        Instant executionThreshold = Instant.now().minusSeconds(30);

        return accountState
                .defaultIfEmpty(AccountState.builder().build())
                .flatMapMany(account -> combinedStream
                        .map(candle5m -> {
                            tracker.updateState(candle5m);

                            BigDecimal usdtBalance = account.balance(QUOTE_ASSET);
                            BigDecimal tradeAmount = usdtBalance.multiply(RISK_PERCENTAGE);
                            if (tradeAmount.compareTo(BigDecimal.ZERO) <= 0) {
                                tradeAmount = new BigDecimal("10"); // Minimum fallback transaction reserve
                            }

                            Signal signal = tracker.evaluateSignal(candle5m, symbol, tradeAmount);
                            if (signal.action() != Signal.Action.HOLD) {
                                System.out.println("[SIGNAL MATCHED] " + signal.action() + " triggered! Details: " + signal.reason());
                            }
                            return signal;
                        })
                )
                .filter(signal -> signal.action() != Signal.Action.HOLD)
                .filter(signal -> signal.timestamp().isAfter(executionThreshold))
                .doFinally(signalType -> processedTimestamps.clear());
    }

    /**
     * Internal REST query lookup method for fetching past candle sets from OKX v5 History API.
     */
    private Flux<Candle> fetchOkxHistory(String symbol, int days) {
        long afterMs = LocalDateTime.now().minusDays(days).toInstant(ZoneOffset.UTC).toEpochMilli();
        String okxSymbol = symbol.contains("-") ? symbol : symbol.replace("USDT", "-USDT").replace("USDC", "-USDC");

        System.out.println("[WARM-UP] Fetching historical OKX candles for target range configuration...");

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v5/market/history-candles")
                        .queryParam("instId", okxSymbol)
                        .queryParam("bar", "5m")
                        .queryParam("after", afterMs)
                        .queryParam("limit", "300") // Collects a robust block of rolling candles to properly isolate daily ranges
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .flatMapMany(response -> {
                    if (!"0".equals(response.get("code"))) {
                        System.err.println("[WARM-UP] REST endpoint returned an explicit API code error: " + response.get("msg"));
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
                    System.err.println("[WARM-UP ERROR] Network socket failed to reach external platform: " + e.getMessage());
                    return Flux.empty();
                });
    }
}
