package com.tradingbot;

import com.tradingbot.core.CandleAggregator;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.model.Tick;
import com.tradingbot.strategy.MovingAverageCrossStrategy;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MovingAverageCrossStrategyTest {

    // ---- Helpers ------------------------------------------------------------

    private Candle candle(double close) {
        BigDecimal    c = BigDecimal.valueOf(close);
        LocalDateTime t = LocalDateTime.now();
        return Candle.builder()
            .symbol("BTCUSDT")
            .openTime(t).closeTime(t.plusMinutes(5))
            .open(c).high(c).low(c).close(c)
            .volume(BigDecimal.ONE).tickCount(10)
            .timeframe(Duration.ofMinutes(5))
            .build();
    }

    private Tick tick(double price) {
        return Tick.builder()
            .symbol("BTCUSDT")
            .bid(BigDecimal.valueOf(price - 0.5))
            .ask(BigDecimal.valueOf(price + 0.5))
            .lastPrice(BigDecimal.valueOf(price))
            .volume(BigDecimal.ONE)
            .timestamp(Instant.now())
            .build();
    }

    private Mono<AccountState> account() {
        return Mono.just(AccountState.builder()
            .balances(Map.of("USDT", BigDecimal.valueOf(10000)))
            .openOrders(Map.of())
            .snapshotTime(Instant.now())
            .build());
    }

    // ---- Strategy tests -----------------------------------------------------

    @Test
    void emitsBuyOrSellSignalAfterCross() {
        MovingAverageCrossStrategy strategy =
            new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));

        Flux<Candle> candles = Flux.just(
            candle(100), candle(99), candle(98), candle(97), candle(96),
            candle(100), candle(105), candle(110), candle(115), candle(120)
        );

        StepVerifier.create(strategy.evaluate(candles, account(), "BTCUSDT"))
            .expectNextMatches(s -> s.action() == Signal.Action.BUY
                                 || s.action() == Signal.Action.SELL)
            .thenCancel()
            .verify();
    }

    @Test
    void signalCarriesCorrectSymbol() {
        MovingAverageCrossStrategy strategy =
            new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));

        Flux<Candle> candles = Flux.just(
            candle(100), candle(99), candle(98), candle(97), candle(96),
            candle(105), candle(110), candle(115), candle(120), candle(125)
        );

        StepVerifier.create(strategy.evaluate(candles, account(), "ETHUSDT"))
            .expectNextMatches(s -> "ETHUSDT".equals(s.symbol()))
            .thenCancel()
            .verify();
    }

    @Test
    void signalCarriesUsdtAmount() {
        BigDecimal amount = BigDecimal.valueOf(250);
        MovingAverageCrossStrategy strategy =
            new MovingAverageCrossStrategy(3, 5, amount);

        Flux<Candle> candles = Flux.just(
            candle(100), candle(99), candle(98), candle(97), candle(96),
            candle(105), candle(110), candle(115), candle(120), candle(125)
        );

        StepVerifier.create(strategy.evaluate(candles, account(), "BTCUSDT"))
            .expectNextMatches(s -> amount.compareTo(s.usdtAmount()) == 0)
            .thenCancel()
            .verify();
    }

    @Test
    void flatPriceProducesNoSignals() {
        MovingAverageCrossStrategy strategy =
            new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));

        Flux<Candle> candles = Flux.just(
            candle(100), candle(100), candle(100), candle(100), candle(100),
            candle(100), candle(100), candle(100), candle(100), candle(100)
        );

        StepVerifier.create(strategy.evaluate(candles, account(), "BTCUSDT"))
            .expectNextCount(0)
            .expectComplete()
            .verify();
    }

    // ---- CandleAggregator tests ---------------------------------------------

    @Test
    void aggregatorReturnsNullWhileWindowIsOpen() {
        CandleAggregator agg = new CandleAggregator(Duration.ofMinutes(5));
        // All ticks within the same window — should never return a candle
        assertNull(agg.onTick(tick(100)));
        assertNull(agg.onTick(tick(101)));
        assertNull(agg.onTick(tick(99)));
    }

    @Test
    void aggregatorReturnsCandleWhenWindowRolls() throws InterruptedException {
        // Use a 1-second window so the test completes quickly
        CandleAggregator agg = new CandleAggregator(Duration.ofSeconds(1));

        assertNull(agg.onTick(tick(100)));
        assertNull(agg.onTick(tick(105)));
        assertNull(agg.onTick(tick(98)));

        // Wait for the window to expire
        Thread.sleep(1100);

        // Next tick triggers the close
        Candle closed = agg.onTick(tick(102));
        assertNotNull(closed, "Expected a closed candle after window expiry");
        assertEquals(3, closed.tickCount());
        // midPrice of 100 = (99.5+100.5)/2 = 100.0
        assertEquals(0, closed.open().compareTo(BigDecimal.valueOf(100.0)));
        // high = midPrice of 105 = 105.0
        assertEquals(0, closed.high().compareTo(BigDecimal.valueOf(105.0)));
        // low  = midPrice of 98  = 98.0
        assertEquals(0, closed.low().compareTo(BigDecimal.valueOf(98.0)));
        // close = midPrice of 98 = 98.0  (last tick before window rolled)
        assertEquals(0, closed.close().compareTo(BigDecimal.valueOf(98.0)));
    }

    @Test
    void aggregatorTimeframeAccessor() {
        assertEquals(5,  new CandleAggregator(Duration.ofMinutes(5)).timeframeMinutes());
        assertEquals(15, new CandleAggregator(Duration.ofMinutes(15)).timeframeMinutes());
        assertEquals(60, new CandleAggregator(Duration.ofHours(1)).timeframeMinutes());
    }
}
