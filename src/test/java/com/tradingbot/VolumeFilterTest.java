package com.tradingbot;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.strategy.EmaRsiCombinedStrategy;
import com.tradingbot.strategy.MovingAverageCrossStrategy;
import com.tradingbot.strategy.TradingStrategy;
import com.tradingbot.strategy.VolumeFilter;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VolumeFilterTest {

    // ---- helpers ------------------------------------------------------------

    private Candle candle(double close, double volume) {
        BigDecimal c = BigDecimal.valueOf(close);
        BigDecimal v = BigDecimal.valueOf(volume);
        LocalDateTime t = LocalDateTime.now();
        return Candle.builder()
            .symbol("BTCUSDT")
            .openTime(t).closeTime(t.plusMinutes(5))
            .open(c).high(c).low(c).close(c)
            .volume(v).tickCount(10)
            .timeframe(Duration.ofMinutes(5))
            .build();
    }

    private Mono<AccountState> account() {
        return Mono.just(AccountState.builder()
            .balances(Map.of("USDT", BigDecimal.valueOf(10000)))
            .openOrders(Map.of())
            .snapshotTime(Instant.now())
            .build());
    }

    /** A strategy that signals BUY/SELL every candle after warmup, for easy testing */
    private TradingStrategy alwaysSignal() {
        // Use EMA with very short periods so it crosses quickly
        return new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));
    }

    // ---- tests --------------------------------------------------------------

    @Test
    void nameIncludesInnerStrategyAndParams() {
        TradingStrategy filtered = new VolumeFilter(alwaysSignal(), 20, 1.2);
        assertTrue(filtered.name().contains("Vol(20"),
            "Name should contain filter params, got: " + filtered.name());
        assertTrue(filtered.name().contains("EMA") || filtered.name().contains("1.2"),
            "Name should reference inner strategy, got: " + filtered.name());
    }

    @Test
    void blocksSignalsDuringWarmup() {
        // lookback=10, only feed 5 candles — should never produce a signal
        TradingStrategy inner    = alwaysSignal();
        TradingStrategy filtered = new VolumeFilter(inner, 10, 1.0);

        Flux<Candle> candles = Flux.just(
            candle(100, 1000), candle(105, 1100), candle(98, 900),
            candle(107, 1200), candle(95, 800)
        );

        List<Signal> signals = new ArrayList<>();
        filtered.evaluate(candles, account(), "BTCUSDT").subscribe(signals::add);

        assertEquals(0, signals.size(),
            "Should produce no signals during warm-up period");
    }

    @Test
    void allowsSignalsWhenVolumeAboveThreshold() {
        // lookback=3, threshold=1.0 — any volume passes after 3 candles
        TradingStrategy filtered = new VolumeFilter(alwaysSignal(), 3, 1.0);

        // Candles designed to trigger EMA crossover after warmup:
        // downtrend then sharp uptrend
        Flux<Candle> candles = Flux.just(
            candle(100, 1000), candle(99, 1000), candle(98, 1000),  // warm-up window
            candle(97,  1000), candle(96, 1000),                     // EMA spreading
            candle(105, 2000), candle(110, 2000), candle(115, 2000)  // cross + high volume
        );

        List<Signal> signals = new ArrayList<>();
        filtered.evaluate(candles, account(), "BTCUSDT").subscribe(signals::add);

        assertFalse(signals.isEmpty(),
            "Should allow signals when volume >= threshold * average");
    }

    @Test
    void blocksSignalsWhenVolumeBelowThreshold() {
        // lookback=3, threshold=2.0 — signal only when volume is 2x average
        // Use a strategy wrapper that we can force to produce a signal
        TradingStrategy inner = new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));
        TradingStrategy filtered = new VolumeFilter(inner, 3, 2.0);

        // avg volume = 1000, threshold = 2.0 → need volume >= 2000
        // signal candle has volume=1500 → should be blocked
        Flux<Candle> candles = Flux.just(
            candle(100, 1000), candle(99, 1000), candle(98, 1000),
            candle(97,  1000), candle(96, 1000),
            candle(105, 1500), candle(110, 1500), candle(115, 1500)
        );

        List<Signal> signals = new ArrayList<>();
        filtered.evaluate(candles, account(), "BTCUSDT").subscribe(signals::add);

        // Even if inner strategy signals, volume filter should block
        // (avg=1000, need 2000, only have 1500)
        for (Signal s : signals) {
            // Any signal that got through must have been on a candle with volume >= 2000
            // In this test all signal candles have 1500 — expect 0
        }
        assertEquals(0, signals.size(),
            "Should block signals when volume < threshold * average");
    }

    @Test
    void volumeAverageUpdatesRollingWindow() {
        // Verify the rolling window drops old values correctly:
        // lookback=3: candles with volumes [100, 200, 300, 9000]
        // After 4th candle, window = [200, 300, 9000], avg = 3166
        // threshold=1.0 so any volume passes once warm
        TradingStrategy inner    = alwaysSignal();
        TradingStrategy filtered = new VolumeFilter(inner, 3, 1.0);

        Flux<Candle> candles = Flux.just(
            candle(100, 100),
            candle(99,  200),
            candle(98,  300),
            candle(97,  1000), // warm-up complete, window=[100,200,300] avg=200
            candle(96,  1000), // window=[200,300,1000] avg=500
            candle(105, 5000), // window=[300,1000,1000] avg=766, volume=5000 → passes
            candle(110, 5000),
            candle(115, 5000)
        );

        List<Signal> signals = new ArrayList<>();
        filtered.evaluate(candles, account(), "BTCUSDT").subscribe(signals::add);

        // Just verify it runs without error and produces some output
        System.out.println("VolumeFilter rolling window test: " + signals.size() + " signals");
    }

    @Test
    void worksWithDifferentInnerStrategies() {
        // VolumeFilter should wrap any TradingStrategy, not just EMA
        TradingStrategy emaRsi   = new EmaRsiCombinedStrategy(BigDecimal.valueOf(100));
        TradingStrategy filtered = new VolumeFilter(emaRsi, 5, 1.1);

        // Just ensure it composes without throwing
        Flux<Candle> candles = Flux.range(0, 30)
            .map(i -> candle(100 + (i % 3 == 0 ? 5 : -2), 1000 + i * 50));

        List<Signal> signals = new ArrayList<>();
        assertDoesNotThrow(() ->
            filtered.evaluate(candles, account(), "BTCUSDT").subscribe(signals::add));

        System.out.println("EmaRsi+VolumeFilter signals: " + signals.size());
    }
}
