package com.tradingbot.market;

import com.tradingbot.model.Order;
import reactor.core.publisher.Mono;

/**
 * Extension of {@link Market} for futures venues.
 *
 * <p>Futures-specific operations:
 * <ul>
 *   <li>{@link #setLeverage} — set per-symbol leverage before placing an order</li>
 *   <li>{@link #setPositionMode} — switch between one-way and hedge mode</li>
 *   <li>{@link #closePosition} — close an open position for a symbol</li>
 * </ul>
 *
 * <p>Implementations: {@link BinanceFuturesMarket}, {@link OkxFuturesMarket},
 * {@link NoTradeFuturesMarket}.
 */
public interface FuturesMarket extends Market {

    /**
     * Set leverage for a symbol.
     * Called automatically by {@code TradingEngine} before order submission
     * when a signal carries a {@code leverage} value.
     *
     * @param symbol   e.g. "BTCUSDT"
     * @param leverage e.g. 10 for 10x
     */
    Mono<Void> setLeverage(String symbol, int leverage);

    /**
     * Switch between one-way mode and hedge mode.
     *
     * @param hedgeMode true = hedge (LONG + SHORT simultaneously),
     *                  false = one-way (net position only)
     */
    Mono<Void> setPositionMode(boolean hedgeMode);

    /**
     * Close the entire open position for a symbol on the given side.
     * In one-way mode, {@code positionSide} should be {@code BOTH}.
     */
    Mono<Order> closePosition(String symbol, Order.PositionSide positionSide);
}
