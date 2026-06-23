package com.tradingbot.market;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.market.config.MarketUrls;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.Order;
import com.tradingbot.model.OrderBook;
import com.tradingbot.model.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Paper-trading futures market.
 *
 * <p>Streams live prices from the configured WebSocket, simulates fills,
 * and tracks simulated open positions with unrealised PnL.
 *
 * <p>Balance changes:
 * <ul>
 *   <li>OPEN (BUY/SELL): margin = notional / leverage is reserved from USDT balance</li>
 *   <li>CLOSE (reduceOnly): margin returned + realised PnL credited/debited</li>
 * </ul>
 */
public class NoTradeFuturesMarket implements FuturesMarket {

    private static final Logger log = LoggerFactory.getLogger(NoTradeFuturesMarket.class);

    /** Tracks an open simulated futures position. */
    private record SimPosition(
        String     symbol,
        Order.Side side,
        BigDecimal qty,
        BigDecimal entryPrice,
        int        leverage,
        Order.PositionSide positionSide
    ) {}

    private final ObjectMapper   objectMapper;
    private final MarketUrls     urls;
    private final String         marketName;
    private final BigDecimal     initialUsdt;
    private final BigDecimal     commissionRate;

    private final ConcurrentHashMap<String, BigDecimal>   balances  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SimPosition>  positions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tick>         lastTick  = new ConcurrentHashMap<>();

    private int currentLeverage = 1;
    private boolean hedgeMode   = false;

    public NoTradeFuturesMarket(ObjectMapper objectMapper, MarketUrls urls,
                                 String marketName, BigDecimal initialUsdt,
                                 BigDecimal commissionRate) {
        this.objectMapper   = objectMapper;
        this.urls           = urls;
        this.marketName     = marketName;
        this.initialUsdt    = initialUsdt;
        this.commissionRate = commissionRate;
        balances.put("USDT", initialUsdt);
        log.info("[{}] Paper futures — {} USDT, commission: {}", marketName, initialUsdt, commissionRate);
    }

    @Override public String     name()           { return marketName; }
    @Override public BigDecimal commissionRate() { return commissionRate; }

    // -------------------------------------------------------------------------
    // Tick stream — identical to spot NoTradeMarket
    // -------------------------------------------------------------------------

    @Override
    public Flux<Tick> tickStream(String symbol) {
        URI uri = URI.create(urls.wsUri(symbol));
        Sinks.Many<Tick> sink = Sinks.many().multicast().onBackpressureBuffer();
        ReactorNettyWebSocketClient wsClient = new ReactorNettyWebSocketClient();

        return Flux.defer(() -> {
            wsClient.execute(uri, session ->
                session.receive()
                    .map(msg -> msg.getPayloadAsText())
                    .flatMap(json -> parseTick(json, symbol))
                    .doOnNext(tick -> lastTick.put(symbol, tick))
                    .doOnNext(sink::tryEmitNext)
                    .then()
            ).subscribe();
            return sink.asFlux();
        })
        .retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
            .maxBackoff(Duration.ofSeconds(30))
            .doBeforeRetry(s ->
                log.warn("[{}] WS disconnected, retrying: {}", marketName, s.failure().getMessage())));
    }

    private Mono<Tick> parseTick(String json, String symbol) {
        return Mono.fromCallable(() -> {
            JsonNode n = objectMapper.readTree(json);
            if (!n.has("b") || !n.has("a")) return null;
            return Tick.builder()
                .symbol(symbol)
                .bid(new BigDecimal(n.get("b").asText()))
                .ask(new BigDecimal(n.get("a").asText()))
                .lastPrice(new BigDecimal(n.get("b").asText()))
                .volume(BigDecimal.ZERO)
                .timestamp(Instant.now())
                .build();
        }).onErrorResume(e -> {
            log.error("[{}] Tick parse error: {}", marketName, e.getMessage());
            return Mono.empty();
        }).filter(t -> t != null);
    }

    // -------------------------------------------------------------------------
    // Order book — simulated from live ticks (same as NoTradeMarket)
    // -------------------------------------------------------------------------

    @Override
    public Flux<OrderBook> orderBookStream(String symbol, int depth) {
        return tickStream(symbol).map(tick -> {
            java.util.TreeMap<BigDecimal, BigDecimal> bids = new java.util.TreeMap<>(java.util.Collections.reverseOrder());
            java.util.TreeMap<BigDecimal, BigDecimal> asks = new java.util.TreeMap<>();
            BigDecimal tickSize = tick.ask().subtract(tick.bid())
                .max(new BigDecimal("0.0001"));
            for (int i = 0; i < depth; i++) {
                BigDecimal vol = BigDecimal.valueOf(100.0 / (i + 1));
                bids.put(tick.bid().subtract(tickSize.multiply(BigDecimal.valueOf(i))), vol);
                asks.put(tick.ask().add(tickSize.multiply(BigDecimal.valueOf(i))), vol);
            }
            return new OrderBook(symbol, bids, asks, 0, java.time.Instant.now());
        });
    }

    // -------------------------------------------------------------------------
    // Account
    // -------------------------------------------------------------------------

    @Override
    public Mono<AccountState> fetchAccountState() {
        return Mono.just(AccountState.builder()
            .balances(new ConcurrentHashMap<>(balances))
            .openOrders(Map.of())
            .snapshotTime(Instant.now())
            .build());
    }

    /**
     * Calculates total unrealised PnL across all open positions at given prices.
     * Called by AccountService on every candle close.
     */
    public BigDecimal unrealisedPnl(Map<String, BigDecimal> prices) {
        BigDecimal total = BigDecimal.ZERO;
        for (SimPosition pos : positions.values()) {
            BigDecimal price = prices.get(pos.symbol());
            if (price == null) continue;
            total = total.add(calculatePnl(pos, price));
        }
        return total;
    }

    // -------------------------------------------------------------------------
    // Simulated orders
    // -------------------------------------------------------------------------

    @Override
    public Mono<Order> submitOrder(Order order) {
        return Mono.fromCallable(() -> {
            Tick tick       = lastTick.get(order.symbol());
            BigDecimal price = tick != null ? tick.midPrice()
                : (order.limitPrice() != null ? order.limitPrice() : BigDecimal.ZERO);

            int lev = order.leverage() != null ? order.leverage() : currentLeverage;
            BigDecimal notional = order.quantity().multiply(price);
            BigDecimal margin   = notional.divide(BigDecimal.valueOf(lev), 8, java.math.RoundingMode.HALF_UP);
            BigDecimal fee      = notional.multiply(commissionRate);

            if (Boolean.TRUE.equals(order.reduceOnly())) {
                // Closing a position — return margin + PnL
                String posKey = positionKey(order.symbol(), order.positionSide());
                SimPosition pos = positions.remove(posKey);
                if (pos != null) {
                    BigDecimal pnl = calculatePnl(pos, price);
                    BigDecimal returnedMargin = notional.divide(BigDecimal.valueOf(pos.leverage()),
                        8, java.math.RoundingMode.HALF_UP);
                    balances.merge("USDT",
                        returnedMargin.add(pnl).subtract(fee), BigDecimal::add);
                    log.info("[{}] Position closed: {} {} PnL={} USDT", marketName,
                        pos.side(), pos.symbol(), pnl.setScale(4, java.math.RoundingMode.HALF_UP));
                }
            } else {
                // Opening a position — reserve margin
                if (balances.getOrDefault("USDT", BigDecimal.ZERO).compareTo(margin.add(fee)) < 0) {
                    throw new IllegalStateException("Insufficient USDT margin");
                }
                balances.merge("USDT", margin.add(fee).negate(), BigDecimal::add);
                String posKey = positionKey(order.symbol(), order.positionSide());
                positions.put(posKey, new SimPosition(order.symbol(), order.side(),
                    order.quantity(), price, lev,
                    order.positionSide() != null ? order.positionSide() : Order.PositionSide.BOTH));
                log.info("[{}] Position opened: {} {} qty={} @ {} lev={}x margin={} USDT",
                    marketName, order.side(), order.symbol(), order.quantity(),
                    price, lev, margin.setScale(2, java.math.RoundingMode.HALF_UP));
            }

            return order.toBuilder()
                .exchangeOrderId(UUID.randomUUID().toString())
                .fillPrice(price).filledQuantity(order.quantity())
                .status(Order.Status.FILLED)
                .updatedAt(Instant.now()).build();
        });
    }

    @Override
    public Mono<Order> cancelOrder(String clientOrderId) {
        return Mono.just(Order.builder()
            .clientOrderId(clientOrderId)
            .status(Order.Status.CANCELLED)
            .updatedAt(Instant.now()).build());
    }

    // -------------------------------------------------------------------------
    // Futures-specific
    // -------------------------------------------------------------------------

    @Override
    public Mono<Void> setLeverage(String symbol, int leverage) {
        this.currentLeverage = leverage;
        log.info("[{}] Leverage set to {}x for {}", marketName, leverage, symbol);
        return Mono.empty();
    }

    @Override
    public Mono<Void> setPositionMode(boolean hedgeMode) {
        this.hedgeMode = hedgeMode;
        log.info("[{}] Position mode: {}", marketName, hedgeMode ? "HEDGE" : "ONE-WAY");
        return Mono.empty();
    }

    @Override
    public Mono<Order> closePosition(String symbol, Order.PositionSide positionSide) {
        String posKey = positionKey(symbol, positionSide);
        SimPosition pos = positions.get(posKey);
        if (pos == null) {
            log.warn("[{}] No open position to close for {} {}", marketName, symbol, positionSide);
            return Mono.empty();
        }
        Order closeOrder = Order.builder()
            .symbol(symbol)
            .side(pos.side() == Order.Side.BUY ? Order.Side.SELL : Order.Side.BUY)
            .type(Order.Type.MARKET)
            .quantity(pos.qty())
            .positionSide(positionSide).reduceOnly(true)
            .leverage(pos.leverage())
            .status(Order.Status.PENDING)
            .createdAt(Instant.now()).updatedAt(Instant.now()).build();
        return submitOrder(closeOrder);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private BigDecimal calculatePnl(SimPosition pos, BigDecimal exitPrice) {
        // LONG PnL = qty * (exit - entry); SHORT PnL = qty * (entry - exit)
        BigDecimal priceDiff = pos.side() == Order.Side.BUY
            ? exitPrice.subtract(pos.entryPrice())
            : pos.entryPrice().subtract(exitPrice);
        return pos.qty().multiply(priceDiff, MathContext.DECIMAL64);
    }

    private String positionKey(String symbol, Order.PositionSide side) {
        return symbol + ":" + (side != null ? side : Order.PositionSide.BOTH);
    }
}
