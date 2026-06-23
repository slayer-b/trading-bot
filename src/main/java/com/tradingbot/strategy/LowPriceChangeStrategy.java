package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple 1% reactive crypto strategy.
 * Idea:
 * - Buy when price falls by buyThresholdPct from the current reference price.
 * - Sell when price rises by sellThresholdPct from the last buy/reference price.
 * - Uses candle.close as the trading price signal.
 * - Emits no signal when the threshold is not reached.
 * This is intentionally conservative and stateful. For real trading, add:
 * fees, slippage, exchange minimums, order status handling, persistence,
 * backtesting, and risk limits.
 */
public class LowPriceChangeStrategy implements TradingStrategy {

    private static final MathContext MC = new MathContext(18, RoundingMode.HALF_UP);

    private final BigDecimal buyThresholdPct;
    private final BigDecimal sellThresholdPct;
    private final BigDecimal quoteOrderSize;

    public LowPriceChangeStrategy() {
        this(new BigDecimal("0.01"), new BigDecimal("0.01"), new BigDecimal("25"));
    }

    public LowPriceChangeStrategy(BigDecimal buyThresholdPct,
                                  BigDecimal sellThresholdPct,
                                  BigDecimal quoteOrderSize) {
        this.buyThresholdPct = requirePositive(buyThresholdPct, "buyThresholdPct");
        this.sellThresholdPct = requirePositive(sellThresholdPct, "sellThresholdPct");
        this.quoteOrderSize = requirePositive(quoteOrderSize, "quoteOrderSize");
    }

    public Flux<Signal> generateSignals(Flux<Candle> candles,
                                        Mono<AccountState> accountState,
                                        String symbol) {
        Objects.requireNonNull(candles, "candles");
        Objects.requireNonNull(accountState, "accountState");
        Objects.requireNonNull(symbol, "symbol");

        return accountState.flatMapMany(initialAccount -> {
            StrategyState state = new StrategyState(initialAccount);

            return candles
                    .filter(candle -> symbol.equals(candle.symbol()))
                    .filter(candle -> candle.close() != null && candle.close().signum() > 0)
                    .concatMap(candle -> evaluateCandle(candle, state, symbol));
        });
    }

    private Mono<Signal> evaluateCandle(Candle candle, StrategyState state, String symbol) {
        BigDecimal price = candle.close();

        if (state.referencePrice.get() == null) {
            state.referencePrice.set(price);
            return Mono.empty();
        }

        BigDecimal reference = state.referencePrice.get();
        BigDecimal changePct = price.subtract(reference, MC).divide(reference, MC);

        boolean hasPosition = state.basePosition.signum() > 0;
        boolean canBuy = state.quoteBalance.compareTo(quoteOrderSize) >= 0;

        if (!hasPosition && canBuy && changePct.compareTo(buyThresholdPct.negate()) <= 0) {
            BigDecimal quantity = quoteOrderSize.divide(price, MC);

            state.quoteBalance = state.quoteBalance.subtract(quoteOrderSize, MC);
            state.basePosition = state.basePosition.add(quantity, MC);
            state.referencePrice.set(price);

//            return Mono.just(new Signal(
//                    symbol,
//                    SignalType.BUY,
//                    price,
//                    quantity,
//                    quoteOrderSize,
//                    candle.closeTime(),
//                    "BUY: price dropped " + percent(changePct) + " from reference " + reference
//            ));
            return null;
        }

        if (hasPosition && changePct.compareTo(sellThresholdPct) >= 0) {
            BigDecimal quantity = state.basePosition;
            BigDecimal quoteValue = quantity.multiply(price, MC);

            state.basePosition = BigDecimal.ZERO;
            state.quoteBalance = state.quoteBalance.add(quoteValue, MC);
            state.referencePrice.set(price);

            return null;
//            return Mono.just(new Signal(
//                    Signal.Action.SELL,
//                    symbol,
//                    SignalType.SELL,
//                    price,
//                    quantity,
//                    quoteValue,
//                    candle.closeTime(),
//                    "SELL: price rose " + percent(changePct) + " from reference " + reference
//            ));
        }

        return Mono.empty();
    }

    private static BigDecimal requirePositive(BigDecimal value, String name) {
        if (value == null || value.signum() <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String percent(BigDecimal decimal) {
        return decimal.multiply(new BigDecimal("100"), MC)
                .setScale(4, RoundingMode.HALF_UP)
                .toPlainString() + "%";
    }

    @Override
    public String name() {
        return "LowPriceChange";
    }

    @Override
    public Flux<com.tradingbot.model.Signal> evaluate(Flux<com.tradingbot.model.Candle> candles, Mono<com.tradingbot.model.AccountState> accountState, String symbol) {
        return null;
    }

    private static final class StrategyState {
        private final AtomicReference<BigDecimal> referencePrice = new AtomicReference<>();
        private BigDecimal basePosition;
        private BigDecimal quoteBalance;

        private StrategyState(AccountState accountState) {

            /**
             * Minimal account shape assumed by this example.
             * basePosition = amount of the traded asset, e.g. BTC.
             * quoteBalance = amount of quote currency, e.g. USDT.
             */
            this.basePosition = nullToZero(accountState.balance("BTC"));
            this.quoteBalance = nullToZero(accountState.balance("USDT"));
        }

        private static BigDecimal nullToZero(BigDecimal value) {
            return value == null ? BigDecimal.ZERO : value;
        }
    }

}