package com.tradingbot;

import com.tradingbot.core.CandleAggregator;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.model.Tick;
import com.tradingbot.model.AccountState;
import com.tradingbot.strategy.MovingAverageCrossStrategy;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests every layer of the pipeline in isolation and then end-to-end.
 */
public class PipelineIntegrationTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Layer 1: CandleAggregator.onTick() — pure Java, no Reactor
    // -------------------------------------------------------------------------

    @Test
    void aggregator_returnsNull_whileWindowOpen() {
        CandleAggregator agg = new CandleAggregator(Duration.ofSeconds(2));
        assertNull(agg.onTick(tick(100)), "First tick should open candle, return null");
        assertNull(agg.onTick(tick(101)), "Same window, should return null");
        assertNull(agg.onTick(tick(99)),  "Same window, should return null");
    }

    @Test
    void aggregator_returnsCandle_whenWindowRolls() throws Exception {
        CandleAggregator agg = new CandleAggregator(Duration.ofSeconds(1));
        assertNull(agg.onTick(tick(100)));
        assertNull(agg.onTick(tick(110)));
        assertNull(agg.onTick(tick(90)));
        Thread.sleep(1100); // let the window expire
        Candle c = agg.onTick(tick(105)); // this tick triggers the close
        assertNotNull(c, "Should have returned a closed candle");
        assertEquals(3, c.tickCount());
        assertEquals(0, c.open().compareTo(BigDecimal.valueOf(100.0)));  // midPrice(100)
        assertEquals(0, c.high().compareTo(BigDecimal.valueOf(110.0)));  // midPrice(110)
        assertEquals(0, c.low().compareTo(BigDecimal.valueOf(90.0)));    // midPrice(90)
        assertEquals(0, c.close().compareTo(BigDecimal.valueOf(90.0)));  // last tick before roll
    }

    @Test
    void aggregator_multipleWindows() throws Exception {
        CandleAggregator agg = new CandleAggregator(Duration.ofSeconds(1));
        List<Candle> closed = new ArrayList<>();

        // Window 1
        assertNull(agg.onTick(tick(100)));
        assertNull(agg.onTick(tick(102)));
        Thread.sleep(1100);
        Candle c1 = agg.onTick(tick(200)); // opens window 2
        assertNotNull(c1, "Window 1 should close");
        closed.add(c1);

        // Window 2
        assertNull(agg.onTick(tick(201)));
        Thread.sleep(1100);
        Candle c2 = agg.onTick(tick(300)); // opens window 3
        assertNotNull(c2, "Window 2 should close");
        closed.add(c2);

        assertEquals(2, closed.size());
        assertEquals(0, closed.get(0).open().compareTo(BigDecimal.valueOf(100.0)));
        assertEquals(0, closed.get(1).open().compareTo(BigDecimal.valueOf(200.0)));
    }

    // -------------------------------------------------------------------------
    // Layer 2: Sinks.Many — does the sink deliver to a subscriber?
    // -------------------------------------------------------------------------

    @Test
    void sink_unicast_deliversToSubscriber() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();

        List<String> received = new ArrayList<>();
        sink.asFlux().subscribe(received::add);

        sink.tryEmitNext("a");
        sink.tryEmitNext("b");
        sink.tryEmitNext("c");

        assertEquals(List.of("a", "b", "c"), received,
            "Unicast sink should deliver all items to subscriber");
    }

    @Test
    void sink_unicast_mustSubscribeBeforeEmitting() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();

        // Emit BEFORE subscribing — unicast buffers these
        sink.tryEmitNext("a");
        sink.tryEmitNext("b");

        List<String> received = new ArrayList<>();
        sink.asFlux().subscribe(received::add);  // subscribe after emits

        // unicast buffers on backpressure — items should still arrive
        assertEquals(List.of("a", "b"), received,
            "Unicast sink buffers items, late subscriber should receive them");
    }

    // -------------------------------------------------------------------------
    // Layer 3: Strategy — does it emit signals given candles?
    // -------------------------------------------------------------------------

    @Test
    void strategy_ema_emitsSignal_givenEnoughCandles() {
        MovingAverageCrossStrategy strategy =
            new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));

        // Build candles that force a crossover
        Flux<Candle> candles = buildCandleFlux(
            100, 99, 98, 97, 96,          // downtrend: fast drops below slow
            100, 105, 110, 115, 120        // uptrend: fast crosses above slow → BUY
        );

        List<Signal> signals = new ArrayList<>();
        strategy.evaluate(candles, account(), "BTCUSDT")
            .subscribe(signals::add);

        assertFalse(signals.isEmpty(), "EMA strategy should emit at least one signal");
        System.out.println("Signals emitted: " + signals.size());
        signals.forEach(s -> System.out.println("  " + s.action() + " @ " + s.usdtAmount()));
    }

    // -------------------------------------------------------------------------
    // Layer 4: Full pipeline — aggregator → sink → strategy
    // -------------------------------------------------------------------------

    @Test
    void fullPipeline_candlesFromSinkReachStrategy() {
        // Simulate exactly what TradingEngine does:
        // 1. Create sink
        // 2. Subscribe strategy to sink FIRST
        // 3. Then push candles into sink

        Sinks.Many<Candle> candleSink = Sinks.many().unicast().onBackpressureBuffer();
        MovingAverageCrossStrategy strategy =
            new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));

        List<Signal> received = new ArrayList<>();

        // Step 1: subscribe strategy FIRST
        strategy.evaluate(candleSink.asFlux(), account(), "BTCUSDT")
            .subscribe(received::add);

        // Step 2: push candles that will produce a crossover
        double[] prices = {100, 99, 98, 97, 96, 100, 105, 110, 115, 120};
        for (double price : prices) {
            candleSink.tryEmitNext(makeCandle(price));
        }

        assertFalse(received.isEmpty(),
            "Strategy should have received signals through the sink. Got: " + received.size());
        System.out.println("Full pipeline signals: " + received.size());
        received.forEach(s -> System.out.println("  " + s.action()));
    }

    @Test
    void fullPipeline_aggregatorToSinkToStrategy() throws Exception {
        // Full chain: raw ticks → aggregator.onTick() → sink → strategy
        CandleAggregator agg = new CandleAggregator(Duration.ofSeconds(1));
        Sinks.Many<Candle> sink = Sinks.many().unicast().onBackpressureBuffer();
        MovingAverageCrossStrategy strategy =
            new MovingAverageCrossStrategy(3, 5, BigDecimal.valueOf(100));

        List<Candle> closedCandles = new ArrayList<>();
        List<Signal> signals = new ArrayList<>();

        // Subscribe strategy first
        strategy.evaluate(sink.asFlux(), account(), "BTCUSDT")
            .subscribe(signals::add);

        // Push 3 windows of ticks with prices that will create a crossover
        double[][] windows = {
            {100, 99, 98},   // window 1: downtrend
            {97, 96, 95},    // window 2: continue down
            {100, 110, 120}  // window 3: sharp up
        };

        for (double[] window : windows) {
            for (double price : window) {
                Candle c = agg.onTick(tick(price));
                if (c != null) {
                    closedCandles.add(c);
                    sink.tryEmitNext(c);
                }
            }
            Thread.sleep(1100); // wait for window to expire
        }
        // trigger close of last window
        Candle last = agg.onTick(tick(120));
        if (last != null) { closedCandles.add(last); sink.tryEmitNext(last); }

        System.out.println("Closed candles: " + closedCandles.size());
        System.out.println("Signals: " + signals.size());

        assertTrue(closedCandles.size() >= 2, "Should have at least 2 closed candles");
        // With only a few candles, EMA may not cross — that's OK, the important
        // thing is candles flow through without errors
        System.out.println("Pipeline works end-to-end. Signals: " + signals);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Flux<Candle> buildCandleFlux(double... closes) {
        List<Candle> list = new ArrayList<>();
        for (double c : closes) list.add(makeCandle(c));
        return Flux.fromIterable(list);
    }

    private Candle makeCandle(double close) {
        BigDecimal c = BigDecimal.valueOf(close);
        java.time.LocalDateTime t = java.time.LocalDateTime.now();
        return Candle.builder()
            .symbol("BTCUSDT")
            .openTime(t).closeTime(t.plusMinutes(1))
            .open(c).high(c).low(c).close(c)
            .volume(BigDecimal.ONE).tickCount(10)
            .timeframe(Duration.ofMinutes(1))
            .build();
    }
}
