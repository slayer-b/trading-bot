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
 * EMA crossover strategy operating on completed candles.
 *
 * <p>Uses candle <em>close</em> price for EMA calculation — the standard
 * approach used by almost all real trading systems.
 *
 * <ul>
 *   <li>BUY  when fast EMA crosses above slow EMA</li>
 *   <li>SELL when fast EMA crosses below slow EMA</li>
 * </ul>
 *
 * <p>Default: fast=9, slow=21 candles. On a 5m timeframe this means the
 * fast EMA covers 45 minutes and the slow EMA covers 105 minutes of history
 * — far more meaningful than reacting to individual ticks.
 */
public class MovingAverageCrossStrategy implements TradingStrategy {

    private static final Logger log = LoggerFactory.getLogger(MovingAverageCrossStrategy.class);

    private final int        fastPeriod;
    private final int        slowPeriod;
    private final BigDecimal usdtAmount;

    private BigDecimal fastEma = null;
    private BigDecimal slowEma = null;

    public MovingAverageCrossStrategy(int fastPeriod, int slowPeriod, BigDecimal usdtAmount) {
        if (fastPeriod >= slowPeriod) throw new IllegalArgumentException("fastPeriod must be < slowPeriod");
        this.fastPeriod = fastPeriod;
        this.slowPeriod = slowPeriod;
        this.usdtAmount = usdtAmount;
    }

    public MovingAverageCrossStrategy(BigDecimal usdtAmount) {
        this(9, 21, usdtAmount);
    }

    @Override
    public String name() {
        return "EMA-Cross(%d/%d)".formatted(fastPeriod, slowPeriod);
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        return candles
            .map(candle -> {
                Signal.Action action = processCandle(candle);
                log.debug("[{}] Candle {} C={} fast={} slow={} → {}",
                    name(), candle.openTime(), candle.close(), fastEma, slowEma, action);
                return action;
            })
            .filter(action -> action != Signal.Action.HOLD)
            .map(action -> Signal.builder()
                .action(action)
                .symbol(symbol)
                .usdtAmount(usdtAmount)
                .reason("EMA-Cross(%d/%d) — fast=%.2f slow=%.2f"
                    .formatted(fastPeriod, slowPeriod, fastEma, slowEma))
                .timestamp(Instant.now())
                .build())
            .doOnNext(s -> log.info("[{}] Signal: {} — {}", name(), s.action(), s.reason()));
    }

    // -------------------------------------------------------------------------
    // EMA on candle close prices
    // -------------------------------------------------------------------------

    private Signal.Action processCandle(Candle candle) {
        BigDecimal close = candle.close();

        if (fastEma == null) {
            fastEma = close;
            slowEma = close;
            return Signal.Action.HOLD;
        }

        BigDecimal mf = BigDecimal.valueOf(2.0 / (fastPeriod + 1));
        BigDecimal ms = BigDecimal.valueOf(2.0 / (slowPeriod + 1));
        MathContext mc = MathContext.DECIMAL64;

        BigDecimal newFast = close.multiply(mf, mc).add(fastEma.multiply(BigDecimal.ONE.subtract(mf), mc));
        BigDecimal newSlow = close.multiply(ms, mc).add(slowEma.multiply(BigDecimal.ONE.subtract(ms), mc));

        boolean wasFastAbove = fastEma.compareTo(slowEma) > 0;
        boolean isFastAbove  = newFast.compareTo(newSlow) > 0;

        fastEma = newFast;
        slowEma = newSlow;

        if (!wasFastAbove && isFastAbove)  return Signal.Action.BUY;
        if (wasFastAbove  && !isFastAbove) return Signal.Action.SELL;
        return Signal.Action.HOLD;
    }
}
