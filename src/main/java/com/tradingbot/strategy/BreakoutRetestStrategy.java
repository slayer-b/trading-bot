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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BreakoutRetestStrategy implements TradingStrategy {

    private static final BigDecimal RISK_PERCENTAGE = new BigDecimal("0.02");
    private static final String QUOTE_ASSET = "USDT";

    private final WebClient webClient = WebClient.builder().baseUrl("https://www.okx.com").build();

    @Override
    public String name() {
        return "breakout_retest_daily_15m_5m";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        System.out.println("[ENGINE] Strategy evaluate pipeline started for instrument: " + symbol);

        MarketStateTracker tracker = new MarketStateTracker();
        var processedTimestamps = ConcurrentHashMap.<LocalDateTime>newKeySet();

        // 1. Fetch history and collect it completely into a synchronous list first
        return fetchOkxHistory(symbol, 2)
                .collectList()
                .flatMapMany(historicalList -> {
                    System.out.println("[WARM-UP COMPLETED] Successfully replayed "
                            + historicalList.size() + " historical candles into tracker for symbol: " + symbol);

                    // Synchronously ingest all historical candles into the state tracker before touching live data
                    for (Candle hc : historicalList) {
                        tracker.updateState(hc);
                        processedTimestamps.add(hc.openTime());
                    }

                    // Guard threshold: dynamically created after the warm-up block has successfully finished loading
                    final Instant executionThreshold = Instant.now().minusSeconds(30);

                    // 2. Now seamlessly chain and process the incoming live candle stream
                    return accountState
                            .defaultIfEmpty(AccountState.builder().build())
                            .flatMapMany(account -> candles
                                    // Prevent duplication on overlapping windows
                                    .filter(candle5m -> processedTimestamps.add(candle5m.openTime()))
                                    .map(candle5m -> {
                                        tracker.updateState(candle5m);

                                        BigDecimal usdtBalance = account.balance(QUOTE_ASSET);
                                        BigDecimal tradeAmount = usdtBalance.multiply(RISK_PERCENTAGE);
                                        if (tradeAmount.compareTo(BigDecimal.ZERO) <= 0) {
                                            tradeAmount = new BigDecimal("10");
                                        }

                                        Signal signal = tracker.evaluateSignal(candle5m, symbol, tradeAmount);
                                        if (signal.action() != Signal.Action.HOLD) {
                                            System.out.println("[SIGNAL MATCHED] " + signal.action()
                                                    + " triggered on " + symbol + "! Details: " + signal.reason());
                                        }
                                        return signal;
                                    })
                            )
                            .filter(signal -> signal.action() != Signal.Action.HOLD)
                            // Strictly verify that the generated trigger happened in real-time, not in the past
                            .filter(signal -> signal.timestamp().isAfter(executionThreshold));
                })
                .doFinally(signalType -> processedTimestamps.clear());
    }


    private Flux<Candle> fetchOkxHistory(String symbol, int days) {
        long afterMs = LocalDateTime.now().minusDays(days).toInstant(ZoneOffset.UTC).toEpochMilli();
        String okxSymbol = symbol.contains("-") ? symbol : symbol.replace("USDT", "-USDT").replace("USDC", "-USDC");

        System.out.println("[WARM-UP] Fetching historical OKX candles for symbol: " + symbol);

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v5/market/history-candles")
                        .queryParam("instId", okxSymbol)
                        .queryParam("bar", "5m")
                        .queryParam("after", afterMs)
                        .queryParam("limit", "300")
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
                })
                .flatMapMany(response -> {
                    if (!"0".equals(response.get("code"))) {
                        System.err.println("[WARM-UP ERROR] OKX API returned an explicit code error: " + response.get("msg"));
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
                            .sort(Comparator.comparing(Candle::openTime));
                })
                .onErrorResume(e -> {
                    System.err.println("[WARM-UP ERROR] Network socket failed to reach external platform for " + symbol + ": " + e.getMessage());
                    return Flux.empty();
                });
    }
}
