package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Instant;

/**
 * Combined EMA crossover + RSI confirmation strategy.
 *
 * <h3>Trading rules</h3>
 * <ul>
 *   <li><b>BUY</b>  — fast EMA crosses above slow EMA
 *                     AND RSI &lt; {@code rsiOverbought} (not already overbought)</li>
 *   <li><b>SELL</b> — fast EMA crosses below slow EMA
 *                     AND RSI &gt; {@code rsiOversold} (not already oversold)</li>
 * </ul>
 *
 * <p>The RSI filter eliminates the most common EMA false signals:
 * <ul>
 *   <li>A bullish EMA cross when RSI is already at 75+ is likely exhausted — skip it</li>
 *   <li>A bearish EMA cross when RSI is already at 25− is likely oversold — skip it</li>
 * </ul>
 *
 * <h3>Defaults</h3>
 * EMA fast=9, slow=21 · RSI period=14, oversold=35, overbought=65
 *
 * <p>The overbought/oversold thresholds are intentionally tighter than the
 * classic 30/70 — we only want to trade in the middle of the RSI range where
 * momentum is cleanest.
 */
public class EmaRsiCombinedStrategy implements TradingStrategy {

    private static final Logger     log = LoggerFactory.getLogger(EmaRsiCombinedStrategy.class);
    private static final MathContext MC  = MathContext.DECIMAL64;

    private final int        emaFast;
    private final int        emaSlow;
    private final int        rsiPeriod;
    private final double     rsiOversold;
    private final double     rsiOverbought;
    private final BigDecimal usdtAmount;

    // EMA state
    private BigDecimal fastEma = null;
    private BigDecimal slowEma = null;

    // RSI state (Wilder smoothing)
    private BigDecimal prevClose = null;
    private BigDecimal avgGain   = null;
    private BigDecimal avgLoss   = null;
    private int        rsiCount  = 0;
    private double     sumGain   = 0;
    private double     sumLoss   = 0;

    public EmaRsiCombinedStrategy(int emaFast, int emaSlow,
                                   int rsiPeriod, double rsiOversold, double rsiOverbought,
                                   BigDecimal usdtAmount) {
        if (emaFast >= emaSlow) throw new IllegalArgumentException("emaFast must be < emaSlow");
        this.emaFast       = emaFast;
        this.emaSlow       = emaSlow;
        this.rsiPeriod     = rsiPeriod;
        this.rsiOversold   = rsiOversold;
        this.rsiOverbought = rsiOverbought;
        this.usdtAmount    = usdtAmount;
    }

    /** Defaults: EMA 9/21, RSI 14, oversold=35, overbought=65 */
    public EmaRsiCombinedStrategy(BigDecimal usdtAmount) {
        this(9, 21, 14, 35.0, 65.0, usdtAmount);
    }

    @Override
    public String name() {
        return "EMA(%d/%d)+RSI(%d)".formatted(emaFast, emaSlow, rsiPeriod);
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        return candles
            .flatMap(candle -> {
                Signal signal = processCandle(candle, symbol);
                return signal != null ? Flux.just(signal) : Flux.empty();
            });
    }

    // -------------------------------------------------------------------------
    // Core logic
    // -------------------------------------------------------------------------

    private Signal processCandle(Candle candle, String symbol) {
        BigDecimal close = candle.close();

        // --- EMA ---
        if (fastEma == null) {
            fastEma = close;
            slowEma = close;
        } else {
            fastEma = ema(close, fastEma, emaFast);
            slowEma = ema(close, slowEma, emaSlow);
        }
        boolean fastAboveSlow = fastEma.compareTo(slowEma) > 0;

        // --- RSI ---
        Double rsi = computeRsi(close);

        log.debug("[{}] C={} fast={} slow={} RSI={}",
            name(), close, fastEma.doubleValue(), slowEma.doubleValue(),
            rsi != null ? "%.1f".formatted(rsi) : "warming");

        if (rsi == null) return null;   // still warming up

        // --- Signal ---
        // We need to detect EMA crossover: compare current vs previous relationship.
        // We track this via a stored flag updated each candle.
        Signal.Action action = evaluateSignal(fastAboveSlow, rsi, close);
        if (action == Signal.Action.HOLD) return null;

        String reason = "%s — fast=%.2f slow=%.2f RSI=%.1f"
            .formatted(name(), fastEma.doubleValue(), slowEma.doubleValue(), rsi);
        log.info("[{}] Signal: {} — {}", name(), action, reason);

        return Signal.builder()
            .action(action)
            .symbol(symbol)
            .usdtAmount(usdtAmount)
            .reason(reason)
            .timestamp(Instant.now())
            .build();
    }

    // -------------------------------------------------------------------------
    // EMA crossover detection — track previous fast/slow relationship
    // -------------------------------------------------------------------------

    private Boolean prevFastAboveSlow = null;

    private Signal.Action evaluateSignal(boolean fastAboveSlow, double rsi, BigDecimal close) {
        if (prevFastAboveSlow == null) {
            prevFastAboveSlow = fastAboveSlow;
            return Signal.Action.HOLD;
        }

        boolean crossedUp   = !prevFastAboveSlow && fastAboveSlow;
        boolean crossedDown =  prevFastAboveSlow && !fastAboveSlow;
        prevFastAboveSlow = fastAboveSlow;

        if (crossedUp && rsi < rsiOverbought) {
            log.debug("[{}] BUY confirmed: EMA crossed up, RSI={} < {}", name(), rsi, rsiOverbought);
            return Signal.Action.BUY;
        }
        if (crossedDown && rsi > rsiOversold) {
            log.debug("[{}] SELL confirmed: EMA crossed down, RSI={} > {}", name(), rsi, rsiOversold);
            return Signal.Action.SELL;
        }
        if (crossedUp) {
            log.info("[{}] EMA crossed up but RSI={} >= {} (overbought) — signal filtered",
                name(), rsi, rsiOverbought);
        }
        if (crossedDown) {
            log.info("[{}] EMA crossed down but RSI={} <= {} (oversold) — signal filtered",
                name(), rsi, rsiOversold);
        }

        return Signal.Action.HOLD;
    }

    // -------------------------------------------------------------------------
    // Wilder RSI
    // -------------------------------------------------------------------------

    private Double computeRsi(BigDecimal close) {
        if (prevClose == null) {
            prevClose = close;
            return null;
        }
        double change = close.subtract(prevClose).doubleValue();
        double gain   = Math.max(change, 0);
        double loss   = Math.max(-change, 0);
        prevClose = close;
        rsiCount++;

        if (rsiCount <= rsiPeriod) {
            sumGain += gain;
            sumLoss += loss;
            if (rsiCount < rsiPeriod) return null;
            avgGain = BigDecimal.valueOf(sumGain / rsiPeriod);
            avgLoss = BigDecimal.valueOf(sumLoss / rsiPeriod);
        } else {
            double a = 1.0 / rsiPeriod;
            avgGain  = BigDecimal.valueOf(avgGain.doubleValue() * (1 - a) + gain * a);
            avgLoss  = BigDecimal.valueOf(avgLoss.doubleValue() * (1 - a) + loss * a);
        }

        if (avgLoss.doubleValue() == 0) return 100.0;
        double rs = avgGain.doubleValue() / avgLoss.doubleValue();
        return 100.0 - (100.0 / (1 + rs));
    }

    // -------------------------------------------------------------------------
    // EMA helper
    // -------------------------------------------------------------------------

    private BigDecimal ema(BigDecimal price, BigDecimal prevEma, int period) {
        BigDecimal multiplier = BigDecimal.valueOf(2.0 / (period + 1));
        return price.multiply(multiplier, MC)
                    .add(prevEma.multiply(BigDecimal.ONE.subtract(multiplier), MC));
    }
}
