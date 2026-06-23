package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.OrderBook;
import com.tradingbot.model.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Order book imbalance strategy — the only genuinely forward-looking signal
 * in this codebase.
 *
 * <h3>How it works</h3>
 * Subscribes to the live order book stream independently of candles.
 * Computes weighted bid/ask imbalance across the top {@code bookDepth} levels.
 * When imbalance stays above {@code buyThreshold} for {@code confirmationTicks}
 * consecutive updates → BUY signal.
 * When imbalance stays below {@code sellThreshold} for the same → SELL signal.
 *
 * <h3>Why imbalance predicts price</h3>
 * If 80% of limit orders are bids, market makers will push their ask up to
 * extract more from buyers. This price pressure manifests in the next few
 * seconds/minutes before any lagging indicator can detect it.
 *
 * <h3>Parameters</h3>
 * <ul>
 *   <li>{@code bookDepth}         — how many levels to consider (default 10)</li>
 *   <li>{@code buyThreshold}      — imbalance above this → bullish (default 0.65)</li>
 *   <li>{@code sellThreshold}     — imbalance below this → bearish (default 0.35)</li>
 *   <li>{@code confirmationTicks} — consecutive ticks required (default 3, reduces noise)</li>
 *   <li>{@code minSpreadBps}      — skip signal if spread too wide (default 5 bps)</li>
 * </ul>
 *
 * <h3>Candle interface</h3>
 * This strategy implements {@link TradingStrategy} (candle-based interface) but
 * ignores the candle stream entirely — it drives itself from the order book.
 * The candle flux is kept alive but unused so the engine wiring stays unchanged.
 * Order book data is injected via {@link #setOrderBookFlux(Flux)}.
 */
public class OrderBookImbalanceStrategy implements TradingStrategy {

    private static final Logger log = LoggerFactory.getLogger(OrderBookImbalanceStrategy.class);

    private final int        bookDepth;
    private final double     buyThreshold;
    private final double     sellThreshold;
    private final int        confirmationTicks;
    private final double     minSpreadBps;
    private final BigDecimal usdtAmount;

    // Injected by TradingEngine before evaluate() is called
    private Flux<OrderBook> orderBookFlux;

    // Confirmation state
    private int    bullishCount  = 0;
    private int    bearishCount  = 0;
    private double lastImbalance = 0.5;

    // Prevent re-entering the same direction without an opposing signal first
    private final AtomicReference<Signal.Action> lastSignal =
        new AtomicReference<>(Signal.Action.HOLD);

    public OrderBookImbalanceStrategy(int bookDepth, double buyThreshold, double sellThreshold,
                                       int confirmationTicks, double minSpreadBps,
                                       BigDecimal usdtAmount) {
        this.bookDepth         = bookDepth;
        this.buyThreshold      = buyThreshold;
        this.sellThreshold     = sellThreshold;
        this.confirmationTicks = confirmationTicks;
        this.minSpreadBps      = minSpreadBps;
        this.usdtAmount        = usdtAmount;
    }

    /** Standard defaults */
    public OrderBookImbalanceStrategy(BigDecimal usdtAmount) {
        this(10, 0.65, 0.35, 3, 5.0, usdtAmount);
    }

    @Override
    public String name() {
        return "OBImbalance(%d,%.2f/%.2f,conf=%d)"
            .formatted(bookDepth, buyThreshold, sellThreshold, confirmationTicks);
    }

    /**
     * Must be called by the engine before {@link #evaluate} to inject the
     * live order book stream for this symbol.
     */
    public void setOrderBookFlux(Flux<OrderBook> flux) {
        this.orderBookFlux = flux;
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState,
                                  String symbol) {
        if (orderBookFlux == null) {
            log.error("[{}] orderBookFlux not set — call setOrderBookFlux() before evaluate()",
                name());
            return Flux.empty();
        }

        // Keep candle subscription alive (engine requires it) but ignore data
        candles.subscribe(c -> {}, e -> {});

        Sinks.Many<Signal> signalSink = Sinks.many().unicast().onBackpressureBuffer();

        orderBookFlux.subscribe(
            book -> {
                Signal signal = processBook(book, symbol);
                if (signal != null) signalSink.tryEmitNext(signal);
            },
            error -> log.error("[{}] Order book stream error: {}", name(), error.getMessage(), error)
        );

        return signalSink.asFlux();
    }

    // -------------------------------------------------------------------------
    // Core logic — called on every order book update (every 100ms)
    // -------------------------------------------------------------------------

    private Signal processBook(OrderBook book, String symbol) {
        // Skip if spread is too wide (illiquid market or flash event)
        if (book.spreadBps() > minSpreadBps) {
            log.debug("[{}] Spread too wide: {} bps", name(), book.spreadBps());
            bullishCount = 0;
            bearishCount = 0;
            return null;
        }

        double imbalance = book.weightedImbalance(bookDepth);
        lastImbalance = imbalance;

        log.debug("[{}] imbalance={} bullish={} bearish={}",
            name(), imbalance, bullishCount, bearishCount);

        // Accumulate confirmation ticks
        if (imbalance >= buyThreshold) {
            bullishCount++;
            bearishCount = 0;
        } else if (imbalance <= sellThreshold) {
            bearishCount++;
            bullishCount = 0;
        } else {
            bullishCount = 0;
            bearishCount = 0;
            return null;
        }

        // Require consecutive confirmation ticks before signalling
        if (bullishCount >= confirmationTicks
                && lastSignal.get() != Signal.Action.BUY) {
            bullishCount = 0;
            lastSignal.set(Signal.Action.BUY);
            String reason = "%s BUY — imbalance=%.4f (>%.2f) for %d ticks | spread=%.2fbps"
                .formatted(name(), imbalance, buyThreshold, confirmationTicks, book.spreadBps());
            log.info("[{}] {}", name(), reason);
            return Signal.builder()
                .action(Signal.Action.BUY)
                .symbol(symbol)
                .usdtAmount(usdtAmount)
                .reason(reason)
                .timestamp(Instant.now())
                .build();
        }

        if (bearishCount >= confirmationTicks
                && lastSignal.get() != Signal.Action.SELL) {
            bearishCount = 0;
            lastSignal.set(Signal.Action.SELL);
            String reason = "%s SELL — imbalance=%.4f (<%.2f) for %d ticks | spread=%.2fbps"
                .formatted(name(), imbalance, sellThreshold, confirmationTicks, book.spreadBps());
            log.info("[{}] {}", name(), reason);
            return Signal.builder()
                .action(Signal.Action.SELL)
                .symbol(symbol)
                .usdtAmount(usdtAmount)
                .reason(reason)
                .timestamp(Instant.now())
                .build();
        }

        return null;
    }
}
