package com.tradingbot.market.impl;

import com.tradingbot.market.Market;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Order;
import com.tradingbot.model.OrderBook;
import com.tradingbot.model.Tick;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * Simulation/Paper trading market wrapper.
 * Delegates history fetching to the actual chosen underlying exchange market.
 */
public class NoTradeMarket implements Market {

    private final Market targetExchangeMarket; // The real exchange market provider (e.g., OkxMarket)

    public NoTradeMarket(Market targetExchangeMarket) {
        this.targetExchangeMarket = targetExchangeMarket;
    }

    @Override
    public String name() {
        return "notrade-" + (targetExchangeMarket != null ? targetExchangeMarket.name() : "generic");
    }

    @Override
    public BigDecimal commissionRate() {
        return targetExchangeMarket != null ? targetExchangeMarket.commissionRate() : BigDecimal.ZERO;
    }

    @Override
    public Flux<Tick> tickStream(String symbol) {
        // Paper trading logic for ticks stream
        return targetExchangeMarket != null ? targetExchangeMarket.tickStream(symbol) : Flux.empty();
    }

    @Override
    public Flux<OrderBook> orderBookStream(String symbol, int depth) {
        return targetExchangeMarket != null ? targetExchangeMarket.orderBookStream(symbol, depth) : Flux.empty();
    }

    @Override
    public Mono<AccountState> fetchAccountState() {
        return Mono.just(AccountState.builder().build()); // Returns isolated paper state
    }

    @Override
    public Mono<Order> submitOrder(Order order) {
        System.out.println("[SIMULATOR] Order intercepted safely: " + order);
        return Mono.just(order); // Simulate execution
    }

    @Override
    public Mono<Order> cancelOrder(String clientOrderId) {
        return Mono.empty();
    }

    /**
     * Crucial Fix: Delegates history fetch strictly to the real exchange market setup
     */
    @Override
    public Flux<Candle> fetchHistory(String symbol, int days) {
        if (targetExchangeMarket != null) {
            System.out.println("[SIMULATOR] Delegating historical warm-up fetch straight to: " + targetExchangeMarket.name());
            return targetExchangeMarket.fetchHistory(symbol, days);
        }
        return Flux.empty();
    }
}
