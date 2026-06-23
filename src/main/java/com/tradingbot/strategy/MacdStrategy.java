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
 * MACD (Moving Average Convergence Divergence) strategy.
 *
 * <h3>Indicator construction</h3>
 * <pre>
 *   MACD line  = EMA(fast) − EMA(slow)         default: EMA(12) − EMA(26)
 *   Signal line = EMA(signal) of MACD line      default: EMA(9)
 *   Histogram   = MACD line − Signal line
 * </pre>
 *
 * <h3>Trading rules</h3>
 * <ul>
 *   <li><b>BUY</b>  — histogram crosses from negative to positive
 *       (MACD line crosses above signal line = bullish momentum)</li>
 *   <li><b>SELL</b> — histogram crosses from positive to negative
 *       (MACD line crosses below signal line = bearish momentum)</li>
 * </ul>
 *
 * <p>Trading on the histogram zero-cross (rather than just the MACD/signal
 * cross) gives one extra candle of confirmation and reduces whipsaws.
 *
 * <h3>Warm-up period</h3>
 * The slow EMA needs {@code slowPeriod} candles before it stabilises.
 * The signal EMA needs {@code signalPeriod} candles on top of that.
 * No signal is emitted during warm-up.
 * With defaults (12/26/9) on 5-minute candles: ~175 minutes of warm-up.
 */
public class MacdStrategy implements TradingStrategy {

    private static final Logger    log = LoggerFactory.getLogger(MacdStrategy.class);
    private static final MathContext MC = MathContext.DECIMAL64;

    private final int        fastPeriod;
    private final int        slowPeriod;
    private final int        signalPeriod;
    private final BigDecimal usdtAmount;

    // EMA state
    private BigDecimal fastEma   = null;
    private BigDecimal slowEma   = null;
    private BigDecimal signalEma = null;   // EMA of the MACD line

    // Previous histogram value — to detect zero-cross
    private BigDecimal prevHistogram = null;

    // Candle counter for warm-up logging
    private int candleCount = 0;

    public MacdStrategy(int fastPeriod, int slowPeriod, int signalPeriod, BigDecimal usdtAmount) {
        if (fastPeriod >= slowPeriod)
            throw new IllegalArgumentException("fastPeriod must be < slowPeriod");
        this.fastPeriod   = fastPeriod;
        this.slowPeriod   = slowPeriod;
        this.signalPeriod = signalPeriod;
        this.usdtAmount   = usdtAmount;
    }

    /** Standard MACD parameters: 12/26/9 */
    public MacdStrategy(BigDecimal usdtAmount) {
        this(12, 26, 9, usdtAmount);
    }

    @Override
    public String name() {
        return "MACD(%d/%d/%d)".formatted(fastPeriod, slowPeriod, signalPeriod);
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
    // Core MACD logic
    // -------------------------------------------------------------------------

    private Signal processCandle(Candle candle, String symbol) {
        candleCount++;
        BigDecimal close = candle.close();

        // Step 1 — update fast and slow EMAs
        if (fastEma == null) {
            fastEma = close;
            slowEma = close;
            return null;
        }
        fastEma = ema(close, fastEma, fastPeriod);
        slowEma = ema(close, slowEma, slowPeriod);

        // Step 2 — MACD line
        BigDecimal macdLine = fastEma.subtract(slowEma);

        // Step 3 — signal line (EMA of MACD line)
        if (signalEma == null) {
            signalEma = macdLine;
            return null;
        }
        signalEma = ema(macdLine, signalEma, signalPeriod);

        // Step 4 — histogram
        BigDecimal histogram = macdLine.subtract(signalEma);

        log.debug("[{}] C={} MACD={} signal={} hist={}",
            name(), close,
            macdLine.doubleValue(), signalEma.doubleValue(), histogram.doubleValue());

        // Step 5 — detect zero-cross on histogram
        Signal result = null;
        if (prevHistogram != null) {
            boolean wasNegative = prevHistogram.compareTo(BigDecimal.ZERO) < 0;
            boolean isPositive  = histogram.compareTo(BigDecimal.ZERO) > 0;
            boolean wasPositive = prevHistogram.compareTo(BigDecimal.ZERO) > 0;
            boolean isNegative  = histogram.compareTo(BigDecimal.ZERO) < 0;

            if (wasNegative && isPositive) {
                String reason = "%s BUY — histogram crossed +ve: %.5f → %.5f"
                    .formatted(name(), prevHistogram.doubleValue(), histogram.doubleValue());
                log.info("[{}] Signal: BUY — {}", name(), reason);
                result = Signal.builder()
                    .action(Signal.Action.BUY)
                    .symbol(symbol)
                    .usdtAmount(usdtAmount)
                    .reason(reason)
                    .timestamp(Instant.now())
                    .build();

            } else if (wasPositive && isNegative) {
                String reason = "%s SELL — histogram crossed -ve: %.5f → %.5f"
                    .formatted(name(), prevHistogram.doubleValue(), histogram.doubleValue());
                log.info("[{}] Signal: SELL — {}", name(), reason);
                result = Signal.builder()
                    .action(Signal.Action.SELL)
                    .symbol(symbol)
                    .usdtAmount(usdtAmount)
                    .reason(reason)
                    .timestamp(Instant.now())
                    .build();
            }
        }

        prevHistogram = histogram;
        return result;
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
