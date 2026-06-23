package com.tradingbot.market;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Order;
import com.tradingbot.model.OrderBook;
import com.tradingbot.model.Tick;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * Abstraction over any trading venue (real or simulated).
 */
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
}
