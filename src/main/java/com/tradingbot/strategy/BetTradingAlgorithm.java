package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple reactive betting/trading algorithm using EMA crossover + basic risk management.
 *
 * Strategy:
 * - Fast EMA (9) crossing above Slow EMA (21) → LONG signal (bet up)
 * - Fast EMA crossing below Slow EMA → SHORT signal (bet down) or close
 * - Position sizing based on account balance (risk 1-2% per trade)
 * - Only one position at a time for simplicity
 */
public class BetTradingAlgorithm implements TradingStrategy {

    private static final int FAST_EMA_PERIOD = 9;
    private static final int SLOW_EMA_PERIOD = 21;
    private static final BigDecimal RISK_PER_TRADE = new BigDecimal("0.015"); // 1.5% risk per trade
    private static final BigDecimal MIN_TRADE_AMOUNT = new BigDecimal("10"); // e.g. USDT minimum

    /**
     * Main evaluation method as per signature.
     */
    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        // Hold latest account state
        AtomicReference<AccountState> latestAccount = new AtomicReference<>(null);

        // Pre-load account state
        Mono<AccountState> accountMono = accountState.doOnNext(latestAccount::set);

        // Process candles reactively with state (EMA calculation)
        Flux<Signal> signals = candles
                .transform(this::withEmaIndicators)
                .withLatestFrom(accountMono.repeat(), (enrichedCandle, acc) -> {
                    latestAccount.set(acc); // keep updated
                    return Tuples.of(enrichedCandle, acc);
                })
                .filter(tuple -> tuple.getT2() != null)
                .map(tuple -> generateSignal(tuple.getT1(), tuple.getT2(), symbol))
                .filter(signal -> signal != null && signal.action() != Signal.Action.HOLD);

        return signals;
    }

    /**
     * Calculates EMA9 and EMA21 on the candle stream.
     */
    private Flux<EnrichedCandle> withEmaIndicators(Flux<Candle> candles) {
        return candles.scan(
                        Tuples.of(new EmaState(), (EnrichedCandle) null),
                        (tuple, candle) -> {
                            BigDecimal close = candle.close();
                            EmaState state = tuple.getT1();

                            state.emaFast = calculateEma(close, state.emaFast, FAST_EMA_PERIOD);
                            state.emaSlow = calculateEma(close, state.emaSlow, SLOW_EMA_PERIOD);

                            EnrichedCandle enriched = new EnrichedCandle(
                                    candle,
                                    state.emaFast,
                                    state.emaSlow
                            );

                            return Tuples.of(state, enriched);
                        }
                )
                .skip(Math.max(FAST_EMA_PERIOD, SLOW_EMA_PERIOD))
                .map(Tuple2::getT2);
    }

    private BigDecimal calculateEma(BigDecimal currentPrice, BigDecimal previousEma, int period) {
        if (previousEma == null) {
            return currentPrice; // seed with first price
        }
        BigDecimal multiplier = BigDecimal.valueOf(2.0 / (period + 1));
        return currentPrice.multiply(multiplier)
                .add(previousEma.multiply(BigDecimal.ONE.subtract(multiplier)));
    }

    private BigDecimal calculatePositionSize(BigDecimal balance, BigDecimal currentPrice) {
        BigDecimal riskAmount = balance.multiply(RISK_PER_TRADE);
        // For simplicity: position size in quote currency (e.g. USDT)
        // In real system you would calculate contracts/coins based on leverage and stop-loss distance
        BigDecimal size = riskAmount; // conservative

        return size.max(MIN_TRADE_AMOUNT);
    }

    private Signal generateSignal(EnrichedCandle enriched, AccountState account, String symbol) {
        BigDecimal emaFast = enriched.getEmaFast();
        BigDecimal emaSlow = enriched.getEmaSlow();

        if (emaFast == null || emaSlow == null) {
            return null; // not enough data
        }

        // Determine crossover
        int compare = emaFast.compareTo(emaSlow);

        // Get available balance from account
        BigDecimal balance = account.balance("USDT"); // Using the record's balance() method
        if (balance == null || balance.compareTo(BigDecimal.ZERO) <= 0) {
            return null;
        }

        // Calculate position size
        BigDecimal positionSize = calculatePositionSize(balance, enriched.getCandle().close());
        Signal.Action action;
        String reason;

        if (compare > 0) {
            // Fast EMA above Slow EMA → LONG/BUY
            action = Signal.Action.BUY;
            reason = "EMA crossover: Fast (" + emaFast + ") above Slow (" + emaSlow + ")";
        } else if (compare < 0) {
            // Fast EMA below Slow EMA → SELL
            action = Signal.Action.SELL;
            reason = "EMA crossover: Fast (" + emaFast + ") below Slow (" + emaSlow + ")";
        } else {
            return null; // HOLD
        }

        // Build Signal using the record's builder
        return Signal.builder()
                .symbol(symbol)
                .action(action)
                .usdtAmount(positionSize)
                .limitPrice(enriched.getCandle().close())
                .reason(reason)
                .timestamp(Instant.now())
                .leverage(null) // No leverage for spot trading
                .positionSide(null) // Not applicable for spot
                .build();
    }

    @Override
    public String name() {
        return "bet";
    }

    // ==================== Helper Classes ====================

    private static class EmaState {
        BigDecimal emaFast;
        BigDecimal emaSlow;
        BigDecimal previousEmaFast;
        BigDecimal previousEmaSlow;

        void updatePrevious() {
            this.previousEmaFast = this.emaFast;
            this.previousEmaSlow = this.emaSlow;
        }
    }

    /**
     * Candle with EMA values attached.
     */
    private static class EnrichedCandle {
        private final Candle candle;
        private final BigDecimal emaFast;
        private final BigDecimal emaSlow;
        private final BigDecimal previousEmaFast;
        private final BigDecimal previousEmaSlow;

        public EnrichedCandle(Candle candle, BigDecimal emaFast, BigDecimal emaSlow) {
            this.candle = candle;
            this.emaFast = emaFast;
            this.emaSlow = emaSlow;
            // Note: previous values would need proper state management in production
            this.previousEmaFast = null; // simplified - enhance with full history if needed
            this.previousEmaSlow = null;
        }

        public Candle getCandle() { return candle; }
        public BigDecimal getEmaFast() { return emaFast; }
        public BigDecimal getEmaSlow() { return emaSlow; }
        public BigDecimal getPreviousEmaFast() { return previousEmaFast; }
        public BigDecimal getPreviousEmaSlow() { return previousEmaSlow; }
    }
}