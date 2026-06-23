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
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Paper-trading market.
 *
 * <p>Streams live prices from the configured WebSocket endpoint.
 * Order submission is simulated — fills are returned instantly at mid-price.
 *
 * <p>BUG FIX: this class no longer maintains its own balance state.
 * {@link AccountService#applyFill} is the single source of truth for balances,
 * so {@code submitOrder} just returns a filled order and lets AccountService
 * update the books. Keeping two separate balance stores caused the SELL guard
 * to always fail (AccountService saw zero base balance forever).
 *
 * <p>Initial paper balance is seeded via {@link #fetchAccountState()}, which
 * AccountService calls once on startup.
 */
public class NoTradeMarket implements Market {

    private static final Logger log = LoggerFactory.getLogger(NoTradeMarket.class);

    private final ObjectMapper   objectMapper;
    private final MarketUrls     urls;
    private final String         marketName;
    private final BigDecimal     initialUsdt;
    private final BigDecimal     commissionRate;

    /** Latest tick per symbol — used only for fill price simulation. */
    private final ConcurrentHashMap<String, Tick> lastTick = new ConcurrentHashMap<>();

    public NoTradeMarket(ObjectMapper objectMapper, MarketUrls urls,
                         String marketName, BigDecimal initialUsdt, BigDecimal commissionRate) {
        this.objectMapper   = objectMapper;
        this.urls           = urls;
        this.marketName     = marketName;
        this.initialUsdt    = initialUsdt;
        this.commissionRate = commissionRate;
        log.info("[{}] Initialised with {} USDT (paper money), commission rate: {}",
            marketName, initialUsdt, commissionRate);
    }

    @Override
    public String name() { return marketName; }

    @Override
    public BigDecimal commissionRate() { return commissionRate; }

    // -------------------------------------------------------------------------
    // Live tick stream
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
            .doBeforeRetry(sig ->
                log.warn("[{}] WS disconnected, retrying: {}", marketName, sig.failure().getMessage())));
    }

    private Mono<Tick> parseTick(String json, String symbol) {
        return Mono.fromCallable(() -> {
            JsonNode node = objectMapper.readTree(json);
            if (!node.has("b") || !node.has("a")) return null;
            return Tick.builder()
                .symbol(symbol)
                .bid(new BigDecimal(node.get("b").asText()))
                .ask(new BigDecimal(node.get("a").asText()))
                .lastPrice(new BigDecimal(node.get("b").asText()))
                .volume(BigDecimal.ZERO)
                .timestamp(Instant.now())
                .build();
        }).onErrorResume(e -> {
            log.error("[{}] Failed to parse tick: {}", marketName, e.getMessage());
            return Mono.empty();
        }).filter(t -> t != null);
    }

    // -------------------------------------------------------------------------
    // Order book stream — simulated from tick data
    // For paper trading we derive a synthetic order book from each tick:
    // bids spread below best bid, asks spread above best ask.
    // This gives the strategy a realistic imbalance signal to work with.
    // -------------------------------------------------------------------------

    @Override
    public Flux<OrderBook> orderBookStream(String symbol, int depth) {
        return tickStream(symbol)
            .map(tick -> simulateOrderBook(tick, symbol, depth));
    }

    private OrderBook simulateOrderBook(Tick tick, String symbol, int depth) {
        java.util.TreeMap<BigDecimal, BigDecimal> bids = new java.util.TreeMap<>(java.util.Collections.reverseOrder());
        java.util.TreeMap<BigDecimal, BigDecimal> asks = new java.util.TreeMap<>();

        // Simulate depth levels around the spread using a realistic distribution:
        // volume decays as levels move away from the best price
        BigDecimal tickSize = tick.ask().subtract(tick.bid())
            .max(new BigDecimal("0.0001"));

        for (int i = 0; i < depth; i++) {
            BigDecimal bidPrice = tick.bid().subtract(
                tickSize.multiply(BigDecimal.valueOf(i)));
            BigDecimal askPrice = tick.ask().add(
                tickSize.multiply(BigDecimal.valueOf(i)));

            // Volume higher near the best price (realistic order book shape)
            BigDecimal vol = BigDecimal.valueOf(100.0 / (i + 1));
            bids.put(bidPrice, vol);
            asks.put(askPrice, vol);
        }

        return new OrderBook(symbol, bids, asks, 0, java.time.Instant.now());
    }

    @Override
    public Mono<AccountState> fetchAccountState() {
        // Return only the initial USDT balance. AccountService will apply all
        // subsequent fills — we must NOT maintain a parallel balance store here.
        return Mono.just(AccountState.builder()
            .balances(new ConcurrentHashMap<>(Map.of("USDT", initialUsdt)))
            .openOrders(new ConcurrentHashMap<>())
            .snapshotTime(Instant.now())
            .build());
    }

    // -------------------------------------------------------------------------
    // Simulated orders — just compute fill price and return a FILLED order.
    // AccountService.applyFill() handles the actual balance update.
    // -------------------------------------------------------------------------

    @Override
    public Mono<Order> submitOrder(Order order) {
        return Mono.fromCallable(() -> {
            Tick tick = lastTick.get(order.symbol());
            BigDecimal fillPrice = tick != null ? tick.midPrice()
                : (order.limitPrice() != null ? order.limitPrice() : BigDecimal.ZERO);

            Order filled = order.toBuilder()
                .exchangeOrderId(UUID.randomUUID().toString())
                .fillPrice(fillPrice)
                .filledQuantity(order.quantity())
                .status(Order.Status.FILLED)
                .updatedAt(Instant.now())
                .build();

            log.info("[{}] Simulated fill: {} {} {} @ {} (paper)",
                marketName, filled.side(), filled.quantity(), filled.symbol(), fillPrice);
            return filled;
        });
    }

    @Override
    public Mono<Order> cancelOrder(String clientOrderId) {
        return Mono.just(Order.builder()
            .clientOrderId(clientOrderId)
            .status(Order.Status.CANCELLED)
            .updatedAt(Instant.now())
            .build());
    }
}
