package com.tradingbot.core;

import com.tradingbot.account.AccountService;
import com.tradingbot.bot.BotResult;
import com.tradingbot.market.FuturesMarket;
import com.tradingbot.market.Market;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Order;
import com.tradingbot.model.OrderBook;
import com.tradingbot.model.Signal;
import com.tradingbot.model.Tick;
import com.tradingbot.strategy.OrderBookImbalanceStrategy;
import com.tradingbot.strategy.TradingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TradingEngine {

    private static final Logger     log     = LoggerFactory.getLogger(TradingEngine.class);
    private static final BigDecimal MIN_QTY = new BigDecimal("0.00001");

    private final Market             market;
    private final TradingStrategy    strategy;
    private final CandleAggregator   candleAggregator;
    private final AccountService     accountService;
    private final ApplicationContext appContext;
    private final String             symbol;
    private final String             quoteAsset;
    private final String             botName;

    private final AtomicReference<BigDecimal> lastAsk          = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> lastBid          = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> lastBuyPrice     = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> commissionPaid   = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> peakPortfolio    = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> maxDrawdown      = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicInteger wins     = new AtomicInteger(0);
    private final AtomicInteger losses   = new AtomicInteger(0);
    private final AtomicInteger trades   = new AtomicInteger(0);
    private int tickCount = 0;

    private Disposable subscription;

    public TradingEngine(Market market, TradingStrategy strategy,
                         CandleAggregator candleAggregator,
                         AccountService accountService, ApplicationContext appContext,
                         String symbol, String quoteAsset, String botName) {
        this.market           = market;
        this.strategy         = strategy;
        this.candleAggregator = candleAggregator;
        this.accountService   = accountService;
        this.appContext       = appContext;
        this.symbol           = symbol;
        this.quoteAsset       = quoteAsset;
        this.botName          = botName;
    }

    public String botName() { return botName; }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public Mono<Void> start() {
        return accountService.initialize()
            .doOnNext(s -> {
                // Snapshot true starting balance BEFORE any trades
                initialUsdtSnapshot = s.balance(quoteAsset);
                peakPortfolio.set(initialUsdtSnapshot);
                log.info("[{}] Starting — market={}, strategy={}, symbol={}, initial={}",
                    botName, market.name(), strategy.name(), symbol, initialUsdtSnapshot);
            })
            .onErrorMap(e -> { handleFatalError(e); return e; })
            .then(Mono.fromRunnable(this::startPipeline));
    }

    public void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
    }

    /**
     * Builds a {@link BotResult} from current state — called by the orchestrator
     * after simulation time expires.
     */
    private BigDecimal initialUsdtSnapshot = null;

    public BotResult buildResult() {
        AccountState st = accountService.currentSync();
        BigDecimal finalUsdt = st != null
            ? st.totalValueUsdt(Map.of(symbol, lastBid.get()), quoteAsset)
            : BigDecimal.ZERO;
        BigDecimal initial = initialUsdtSnapshot != null ? initialUsdtSnapshot : finalUsdt;

        return new BotResult(
            botName, symbol, strategy.name(), market.name(),
            (int) candleAggregator.timeframeMinutes(),
            initial, finalUsdt,
            trades.get(), wins.get(), losses.get(),
            maxDrawdown.get().multiply(BigDecimal.valueOf(100)).setScale(2, RoundingMode.HALF_UP),
            commissionPaid.get().setScale(4, RoundingMode.HALF_UP)
        );
    }

    // -------------------------------------------------------------------------
    // Pipeline
    // -------------------------------------------------------------------------

    private void startPipeline() {
        // If using OrderBookImbalanceStrategy, inject the live order book stream first
        if (strategy instanceof OrderBookImbalanceStrategy obs) {
            Flux<OrderBook> obFlux = market.orderBookStream(symbol, 20);
            obs.setOrderBookFlux(obFlux);
            log.info("[{}] Order book stream injected (depth=20)", botName);
        }

        Sinks.Many<Candle> candleSink = Sinks.many().unicast().onBackpressureBuffer();

        Flux<Signal> signals = strategy.evaluate(
            candleSink.asFlux(), accountService.current(), symbol);

        subscription = signals
            .filter(s -> s.action() != Signal.Action.HOLD)
            .flatMap(this::handleSignal)
            .onErrorContinue((e, obj) -> {
                if (isSymbolNotFound(e)) exitOnSymbolNotFound();
                else log.error("[{}] Pipeline error: {}", botName, e.getMessage(), e);
            })
            .subscribe(
                order -> log.info("[{}] Order processed: {} {} {}",
                    botName, order.status(), order.side(), order.symbol()),
                error -> log.error("[{}] Fatal pipeline error", botName, error)
            );

        market.tickStream(symbol).subscribe(
            tick -> {
                onTick(tick);
                Candle closed = candleAggregator.onTick(tick);
                if (closed != null) {
                    logPortfolio(closed.close());
                    updateDrawdown(closed.close());
                    Sinks.EmitResult result = candleSink.tryEmitNext(closed);
                    if (result.isFailure()) {
                        log.error("[{}] Failed to emit candle: {}", botName, result);
                    }
                }
            },
            error -> {
                if (isSymbolNotFound(error)) exitOnSymbolNotFound();
                else log.error("[{}] Tick stream error: {}", botName, error.getMessage(), error);
            }
        );

        log.info("[{}] Pipeline active — {}m candles, strategy: {}. First candle ~{}",
            botName, candleAggregator.timeframeMinutes(), strategy.name(), floorToNextWindow());
    }

    // -------------------------------------------------------------------------
    // Tick handling
    // -------------------------------------------------------------------------

    private void onTick(Tick tick) {
        lastAsk.set(tick.ask());
        lastBid.set(tick.bid());
        accountService.updatePrice(symbol, tick.midPrice());
        tickCount++;
        if (tickCount == 1) {
            log.info("[{}] First tick: bid={} ask={}, window closes ~{}",
                botName, tick.bid(), tick.ask(), candleAggregator.currentWindowEnd());
        }
    }

    private void logPortfolio(BigDecimal closePrice) {
        AccountState st = accountService.currentSync();
        if (st == null) return;
        BigDecimal usdt  = st.balance(quoteAsset);
        BigDecimal upnl  = st.unrealisedPnlUsdt();
        BigDecimal total = st.totalValueUsdt(Map.of(symbol, closePrice), quoteAsset);
        if (upnl.compareTo(BigDecimal.ZERO) != 0) {
            log.info("[{}] {} USDT free | uPnL: {} | total: {} USDT",
                botName,
                usdt.setScale(2, RoundingMode.HALF_UP),
                upnl.setScale(4, RoundingMode.HALF_UP),
                total.setScale(2, RoundingMode.HALF_UP));
        } else {
            log.info("[{}] {} USDT free | total: {} USDT",
                botName,
                usdt.setScale(2, RoundingMode.HALF_UP),
                total.setScale(2, RoundingMode.HALF_UP));
        }
    }

    private void updateDrawdown(BigDecimal closePrice) {
        AccountState st = accountService.currentSync();
        if (st == null) return;
        BigDecimal total = st.totalValueUsdt(Map.of(symbol, closePrice), quoteAsset);
        peakPortfolio.updateAndGet(peak ->
            total.compareTo(peak) > 0 ? total : peak);
        BigDecimal peak = peakPortfolio.get();
        if (peak.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal dd = peak.subtract(total).divide(peak, 8, RoundingMode.HALF_UP);
            maxDrawdown.updateAndGet(prev -> dd.compareTo(prev) > 0 ? dd : prev);
        }
    }

    // -------------------------------------------------------------------------
    // Signal → Order
    // -------------------------------------------------------------------------

    private Mono<Order> handleSignal(Signal signal) {
        log.info("[{}] Signal: {} {} USDT — {}",
            botName, signal.action(), signal.usdtAmount(), signal.reason());

        AccountState state = accountService.currentSync();
        if (state == null) {
            log.warn("[{}] Account state not initialised — skipping signal", botName);
            return Mono.empty();
        }

        Order order = buildOrder(signal, state);
        if (order == null) return Mono.empty();

        Mono<Void> leverageSetup = Mono.empty();
        if (market instanceof FuturesMarket fm && signal.leverage() != null) {
            leverageSetup = fm.setLeverage(symbol, signal.leverage());
        }

        accountService.registerOpenOrder(order);
        return leverageSetup
            .then(market.submitOrder(order))
            .flatMap(filled -> {
                accountService.applyFill(filled);
                recordTradeOutcome(filled);
                return Mono.just(filled);
            })
            .onErrorResume(e -> {
                if (isSymbolNotFound(e)) exitOnSymbolNotFound();
                else log.error("[{}] Order submission failed: {}", botName, e.getMessage());
                return Mono.empty();
            });
    }

    /**
     * Records win/loss and commission for a filled order.
     * A SELL is a win if fill price > last buy price.
     */
    private void recordTradeOutcome(Order filled) {
        trades.incrementAndGet();

        // Commission: fill price * qty * rate
        BigDecimal fee = filled.fillPrice()
            .multiply(filled.filledQuantity())
            .multiply(market.commissionRate())
            .setScale(8, RoundingMode.HALF_UP);
        commissionPaid.updateAndGet(prev -> prev.add(fee));

        if (filled.side() == Order.Side.BUY) {
            lastBuyPrice.set(filled.fillPrice());
        } else if (filled.side() == Order.Side.SELL) {
            BigDecimal buyPrice = lastBuyPrice.get();
            if (buyPrice.compareTo(BigDecimal.ZERO) > 0) {
                if (filled.fillPrice().compareTo(buyPrice) > 0) wins.incrementAndGet();
                else                                             losses.incrementAndGet();
            }
        }
    }

    private Order buildOrder(Signal signal, AccountState state) {
        boolean    isBuy = signal.action() == Signal.Action.BUY;
        Order.Side side  = isBuy ? Order.Side.BUY : Order.Side.SELL;

        BigDecimal usdtAmount = signal.usdtAmount();
        if (usdtAmount == null || usdtAmount.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("[{}] Signal has no usdtAmount — skipping", botName);
            return null;
        }

        BigDecimal price = isBuy ? lastAsk.get() : lastBid.get();
        if (price.compareTo(BigDecimal.ZERO) == 0) {
            log.warn("[{}] No price yet for {} — skipping", botName, symbol);
            return null;
        }

        BigDecimal cryptoQty = usdtAmount.divide(price, 6, RoundingMode.DOWN);
        if (cryptoQty.compareTo(MIN_QTY) < 0) {
            log.warn("[{}] Qty {} below minimum — skipping", botName, cryptoQty);
            return null;
        }

        String base = symbol.endsWith(quoteAsset)
            ? symbol.substring(0, symbol.length() - quoteAsset.length()) : symbol;

        if (isBuy) {
            if (state.balance(quoteAsset).compareTo(usdtAmount) < 0) {
                log.warn("[{}] Insufficient {} ({}) for {} USDT — skipping",
                    botName, quoteAsset, state.balance(quoteAsset), usdtAmount);
                return null;
            }
        } else {
            if (state.balance(base).compareTo(cryptoQty) < 0) {
                log.warn("[{}] Insufficient {} ({}) to sell {} — skipping",
                    botName, base, state.balance(base), cryptoQty);
                return null;
            }
        }

        log.info("[{}] Building order: {} {} {} (~{} {})",
            botName, side, cryptoQty, symbol, usdtAmount, quoteAsset);

        return Order.builder()
            .symbol(symbol).side(side)
            .type(signal.limitPrice() != null ? Order.Type.LIMIT : Order.Type.MARKET)
            .quantity(cryptoQty).limitPrice(signal.limitPrice())
            .leverage(signal.leverage()).positionSide(signal.positionSide())
            .status(Order.Status.PENDING)
            .createdAt(Instant.now()).updatedAt(Instant.now())
            .build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private boolean isSymbolNotFound(Throwable e) {
        String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
        return msg.contains("invalid symbol") || msg.contains("symbol not found")
            || msg.contains("-1121") || msg.contains("instrument not found");
    }

    private void exitOnSymbolNotFound() {
        log.error("[{}] Symbol '{}' not found on '{}'. Shutting down.",
            botName, symbol, market.name());
        new Thread(() -> System.exit(SpringApplication.exit(appContext, () -> 1)),
            "symbol-not-found-shutdown").start();
    }

    private void handleFatalError(Throwable e) {
        if (isSymbolNotFound(e)) exitOnSymbolNotFound();
        else log.error("[{}] Fatal startup error: {}", botName, e.getMessage(), e);
    }

    private LocalDateTime floorToNextWindow() {
        LocalDateTime now = LocalDateTime.now();
        long tf  = candleAggregator.timeframeMinutes();
        long min = (now.getMinute() / tf + 1) * tf;
        return now.truncatedTo(ChronoUnit.HOURS).plusMinutes(min);
    }
}
