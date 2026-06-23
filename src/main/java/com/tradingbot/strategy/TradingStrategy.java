package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Pluggable trading strategy.
 *
 * <p>Strategies receive a stream of <em>completed</em> {@link Candle}s —
 * never raw ticks. This ensures signals are based on confirmed price action
 * rather than noise, and that commission can realistically be covered by
 * meaningful moves.
 *
 * <p>The candle timeframe is configured in {@code application.yml} via
 * {@code trading.candleTimeframe} and applied by {@code CandleAggregator}
 * in the engine before the strategy sees any data.
 */
public interface TradingStrategy {

    String name();

    /**
     * @param candles      stream of completed candles for the configured timeframe
     * @param accountState latest account state
     * @param symbol       the instrument symbol (e.g. "BTCUSDT") — use this in signals
     */
    Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol);
}
