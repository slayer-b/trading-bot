package com.tradingbot.market;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Order;
import com.tradingbot.model.OrderBook;
import com.tradingbot.model.Tick;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

public interface Market {

    String name();

    /**
     * Commission rate as a fraction (not percent).
     * e.g. 0.001 = 0.1%. Applied to the received side of every fill.
     */
    BigDecimal commissionRate();

    /** Live best-bid/ask tick stream. */
    Flux<Tick> tickStream(String symbol);

    /**
     * Live order book stream — emits a full snapshot on every update.
     *
     * @param symbol e.g. "BTCUSDT"
     * @param depth  price levels per side to maintain (e.g. 20)
     */
    Flux<OrderBook> orderBookStream(String symbol, int depth);

    Mono<AccountState> fetchAccountState();

    Mono<Order> submitOrder(Order order);

    Mono<Order> cancelOrder(String clientOrderId);

    /**
     * Fetches a historical stream of 5m candles for the specified asset pair directly from this market provider.
     *
     * @param symbol The asset ticker symbol (e.g., "SOLUSDT")
     * @param days   The number of days of history to retrieve
     * @return A stream of sorted candles (from oldest to newest)
     */
    Flux<Candle> fetchHistory(String symbol, int days);
}
