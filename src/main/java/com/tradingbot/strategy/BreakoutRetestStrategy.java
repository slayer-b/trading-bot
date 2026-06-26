package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.market.Market;
import com.tradingbot.market.MarketResolver;
import com.tradingbot.strategy.helper.MarketStateTracker;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;

@Component("breakout_retest_daily_15m_5m") // Explicit bean name mapping matching your application.yml
@Scope("prototype") // CRUCIAL: Ensures a brand new isolated instance is manufactured for every single bot session
public class BreakoutRetestStrategy implements TradingStrategy {

    private static final BigDecimal RISK_PERCENTAGE = new BigDecimal("0.02");
    private static final String QUOTE_ASSET = "USDT";

    private final MarketResolver marketResolver;
    private Market market;

    /**
     * Dependency Injection is now fully operational inside the strategy class.
     * Spring automatically provisions the standalone MarketResolver bean here.
     */
    public BreakoutRetestStrategy(MarketResolver marketResolver) {
        this.marketResolver = marketResolver;
        System.out.println("[STRATEGY] Prototype BreakoutRetestStrategy bean created by Spring IoC context.");
    }

    @Override
    public String name() {
        return "breakout_retest_daily_15m_5m";
    }

    /**
     * Custom initialization gateway to bind the execution pipeline with the determined market name string.
     */
    public void initMarket(String marketConfigName) {
        if (this.marketResolver != null && marketConfigName != null) {
            // Resolve and cache the concrete credentialed market layout dynamically
            this.market = this.marketResolver.resolveMarket(marketConfigName);
        }
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        System.out.println("[ENGINE] Prototype strategy evaluate thread activated for asset: " + symbol);

        MarketStateTracker tracker = new MarketStateTracker();
        var processedTimestamps = ConcurrentHashMap.<LocalDateTime>newKeySet();

        // Safe evaluation path logic execution routing directly via Market abstraction method
        Flux<Candle> historicalCandles = (this.market != null) ? this.market.fetchHistory(symbol, 2) : Flux.empty();

        return historicalCandles
                .collectList()
                .flatMapMany(historicalList -> {
                    System.out.println("[WARM-UP COMPLETED] Replayed "
                            + historicalList.size() + " historical context items into tracker for symbol: " + symbol);

                    for (Candle hc : historicalList) {
                        tracker.updateState(hc);
                        processedTimestamps.add(hc.openTime());
                    }

                    final Instant executionThreshold = Instant.now().minusSeconds(30);

                    return accountState
                            .defaultIfEmpty(AccountState.builder().build())
                            .flatMapMany(account -> candles
                                    .filter(candle5m -> processedTimestamps.add(candle5m.openTime()))
                                    .map(candle5m -> {
                                        tracker.updateState(candle5m);

                                        BigDecimal usdtBalance = account.balance(QUOTE_ASSET);
                                        BigDecimal tradeAmount = usdtBalance.multiply(RISK_PERCENTAGE);
                                        if (tradeAmount.compareTo(BigDecimal.ZERO) <= 0) {
                                            tradeAmount = new BigDecimal("10");
                                        }

                                        Signal signal = tracker.evaluateSignal(candle5m, symbol, tradeAmount);
                                        if (signal.action() != Signal.Action.HOLD) {
                                            System.out.println("[SIGNAL MATCHED] " + signal.action()
                                                    + " triggered on " + symbol + "! Details: " + signal.reason());
                                        }
                                        return signal;
                                    })
                            )
                            .filter(signal -> signal.action() != Signal.Action.HOLD)
                            .filter(signal -> signal.timestamp().isAfter(executionThreshold));
                })
                .switchIfEmpty(Flux.defer(() -> {
                    System.out.println("[WARN] Historical data missing from Market. Falling back to clean real time tracking pipeline for: " + symbol);
                    final Instant threshold = Instant.now().minusSeconds(30);
                    return accountState
                            .defaultIfEmpty(AccountState.builder().build())
                            .flatMapMany(account -> candles
                                    .filter(candle5m -> processedTimestamps.add(candle5m.openTime()))
                                    .map(candle5m -> {
                                        tracker.updateState(candle5m);
                                        return tracker.evaluateSignal(candle5m, symbol, new BigDecimal("10"));
                                    })
                            )
                            .filter(signal -> signal.action() != Signal.Action.HOLD)
                            .filter(signal -> signal.timestamp().isAfter(threshold));
                }))
                .doFinally(signalType -> processedTimestamps.clear());
    }
}
