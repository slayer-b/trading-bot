package com.tradingbot.core;

import com.tradingbot.bot.BotConfig;
import com.tradingbot.bot.BotFactory;
import com.tradingbot.bot.BotResult; // Using your exact domain record class
import com.tradingbot.market.Market;
import com.tradingbot.market.MarketResolver;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Order;
import com.tradingbot.model.Signal;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Core execution engine managing a standalone, live reactive pipeline session.
 * Configured as a prototype to ensure perfect state isolation between multiple running tokens.
 */
@Component
@Scope("prototype")
public class TradingEngine {

    private final BotFactory botFactory;
    private final MarketResolver marketResolver;

    private BotConfig config;
    private Disposable activeSubscription;

    // Performance tracking variables matched precisely to your custom BotResult fields
    private final BigDecimal initialUsdt = new BigDecimal("10000");
    private BigDecimal currentUsdt = new BigDecimal("10000");
    private BigDecimal maxPeakUsdt = new BigDecimal("10000");
    private BigDecimal maxDrawdownPct = BigDecimal.ZERO;
    private BigDecimal commissionPaid = BigDecimal.ZERO;

    private final AtomicInteger totalTrades = new AtomicInteger(0);
    private final AtomicInteger wins = new AtomicInteger(0);
    private final AtomicInteger losses = new AtomicInteger(0);

    // Temporary reference to track the entry price of the last execution block for Win/Loss metrics
    private BigDecimal lastEntryPrice = BigDecimal.ZERO;

    private final Map<String, LocalDateTime> lastExecutedCandlePerSymbol = new ConcurrentHashMap<>();

    public TradingEngine(BotFactory botFactory, MarketResolver marketResolver) {
        this.botFactory = botFactory;
        this.marketResolver = marketResolver;
    }

    /**
     * Provisioning entry gateway used by the factory to map configuration parameters onto the state.
     */
    public void initEngineSession(BotConfig config) {
        this.config = config;
    }

    /**
     * Starts and maintains the live background market data analysis loop.
     */
    public Mono<Void> start() {
        if (this.config == null) {
            return Mono.error(new IllegalStateException("Engine context cannot be booted up without an active configuration state mapped."));
        }

        System.out.println("[ENGINE] Preparing reactive trading loop pipeline for asset: " + config.symbol());

        Market activeMarket = marketResolver.resolveMarket(config.market());
        var strategy = botFactory.createStrategy(config.strategyName(), config.market());

        Flux<Candle> resilientCandleStream = fetchCandleStream(activeMarket, config.symbol());
        Mono<AccountState> reactiveAccountState = activeMarket.fetchAccountState();

        // Assemble the execution stream layout mapping pipeline
        Flux<Order> orderPipeline = strategy.evaluate(resilientCandleStream, reactiveAccountState, config.symbol())
                .filter(signal -> signal.action() != Signal.Action.HOLD)
                .distinctUntilChanged(signal -> signal.action().toString() + "_" + signal.symbol())
                .flatMap(signal -> {
                    LocalDateTime currentCandleTime = LocalDateTime.now();
                    String cacheKey = signal.symbol() + "_" + signal.action();

                    if (lastExecutedCandlePerSymbol.containsKey(cacheKey)) {
                        return Mono.empty();
                    }

                    lastExecutedCandlePerSymbol.put(cacheKey, currentCandleTime);

                    Order order = Order.builder()
                            .symbol(signal.symbol())
                            .side(signal.action() == Signal.Action.BUY ? Order.Side.BUY : Order.Side.SELL)
                            .type(Order.Type.MARKET)
                            .quantity(signal.usdtAmount())
                            .build();

                    // Calculate simulated commission fees if the baseline activeMarket rate is available
                    BigDecimal feeRate = activeMarket.commissionRate() != null ? activeMarket.commissionRate() : new BigDecimal("0.001");
                    BigDecimal currentFee = signal.usdtAmount().multiply(feeRate);
                    commissionPaid = commissionPaid.add(currentFee);

                    // Reconcile mathematical equity fluctuations and metrics for backtesting logs
                    if (signal.action() == Signal.Action.BUY) {
                        currentUsdt = currentUsdt.subtract(signal.usdtAmount()).subtract(currentFee);
                        lastEntryPrice = signal.limitPrice();
                    } else if (signal.action() == Signal.Action.SELL) {
                        currentUsdt = currentUsdt.add(signal.usdtAmount()).subtract(currentFee);

                        // Basic metric reconciliation to classify Wins vs Losses based on past entry reference
                        if (lastEntryPrice.compareTo(BigDecimal.ZERO) > 0) {
                            if (signal.limitPrice().compareTo(lastEntryPrice) > 0) {
                                wins.incrementAndGet();
                            } else {
                                losses.incrementAndGet();
                            }
                        }
                    }
                    totalTrades.incrementAndGet();

                    // Calculate Max Drawdown parameters on the fly
                    maxPeakUsdt = maxPeakUsdt.max(currentUsdt);
                    BigDecimal currentDrawdown = maxPeakUsdt.subtract(currentUsdt);
                    if (currentDrawdown.compareTo(BigDecimal.ZERO) > 0 && maxPeakUsdt.compareTo(BigDecimal.ZERO) > 0) {
                        BigDecimal currentDrawdownPct = currentDrawdown.divide(maxPeakUsdt, 4, RoundingMode.HALF_UP).multiply(new BigDecimal("100"));
                        maxDrawdownPct = maxDrawdownPct.max(currentDrawdownPct);
                    }

                    System.out.println("[ENGINE SUBMITTING] Dispatching order transaction layout: " + order);
                    return activeMarket.submitOrder(order)
                            .onErrorResume(err -> Mono.empty());
                });

        // Store active stream reference so the orchestrator can cleanly interrupt it via .stop()
        return Mono.fromRunnable(() -> this.activeSubscription = orderPipeline.subscribe(
                executedOrder -> System.out.println("[SUCCESS] Order confirmed: " + executedOrder),
                error -> System.err.println("[ENGINE ERROR] Stream failure: " + error.getMessage())
        ));
    }

    /**
     * Stems and unsubscribes the background reactive ticker feed connection safely.
     */
    public void stop() {
        if (activeSubscription != null && !activeSubscription.isDisposed()) {
            System.out.println("[ENGINE] Stopping active session subscription container for asset: " + config.symbol());
            activeSubscription.dispose();
        }
    }

    /**
     * Compiles and builds the production performance metrics report using your exact custom BotResult layout.
     *
     * @return Generated complete BotResult instance
     */
    public BotResult buildResult() {
        if (this.config == null) {
            return new BotResult("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", 5, BigDecimal.ZERO, BigDecimal.ZERO, 0, 0, 0, BigDecimal.ZERO, BigDecimal.ZERO);
        }

        return new BotResult(
                config.name() != null ? config.name() : "breakout_" + config.symbol(),
                config.symbol(),
                config.strategyName(),
                config.market(),
                config.candleTimeframeMinutes() != 0 ? config.candleTimeframeMinutes() : 5,
                initialUsdt,
                currentUsdt.setScale(4, RoundingMode.HALF_UP),
                totalTrades.get(),
                wins.get(),
                losses.get(),
                maxDrawdownPct.setScale(2, RoundingMode.HALF_UP),
                commissionPaid.setScale(4, RoundingMode.HALF_UP)
        );
    }

    private Flux<Candle> fetchCandleStream(Market market, String symbol) {
        return market.fetchHistory(symbol, 1)
                .concatWith(Flux.interval(Duration.ofMinutes(5)).map(tick -> Candle.builder().symbol(symbol).build()))
                .retryWhen(Retry.backoff(5, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofMinutes(1)));
    }
}
