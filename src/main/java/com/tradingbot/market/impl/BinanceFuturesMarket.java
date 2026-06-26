package com.tradingbot.market.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.market.FuturesMarket;
import com.tradingbot.market.HmacSigner;
import com.tradingbot.market.config.MarketUrls;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.Order;
import com.tradingbot.model.OrderBook;
import com.tradingbot.model.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Binance USDT-M Perpetual Futures market.
 *
 * <p>Base URL: {@code https://fapi.binance.com}
 * WS base:    {@code wss://fstream.binance.com/ws}
 *
 * <p>Supports:
 * <ul>
 *   <li>One-way mode  — {@code positionSide = BOTH}</li>
 *   <li>Hedge mode    — {@code positionSide = LONG | SHORT}</li>
 *   <li>Per-order leverage via {@link #setLeverage}</li>
 * </ul>
 *
 * <p>All URLs come from {@link MarketUrls} — nothing hardcoded.
 */
public class BinanceFuturesMarket implements FuturesMarket {

    private static final Logger log = LoggerFactory.getLogger(BinanceFuturesMarket.class);

    private final WebClient    webClient;
    private final ObjectMapper objectMapper;
    private final MarketUrls   urls;
    private final String       marketName;
    private final String       apiKey;
    private final String       secretKey;
    private final BigDecimal   commissionRate;

    public BinanceFuturesMarket(WebClient webClient, ObjectMapper objectMapper,
                                 MarketUrls urls, String marketName,
                                 String apiKey, String secretKey,
                                 BigDecimal commissionRate) {
        this.webClient      = webClient;
        this.objectMapper   = objectMapper;
        this.urls           = urls;
        this.marketName     = marketName;
        this.apiKey         = apiKey;
        this.secretKey      = secretKey;
        this.commissionRate = commissionRate;
    }

    @Override public String      name()           { return marketName; }
    @Override public BigDecimal  commissionRate() { return commissionRate; }

    // -------------------------------------------------------------------------
    // Tick stream — same bookTicker format as spot
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
    // Order book stream — same @depth format as spot Binance
    // -------------------------------------------------------------------------

    @Override
    public Flux<OrderBook> orderBookStream(String symbol, int depth) {
        String stream = symbol.toLowerCase() + "@depth" + depth + "@100ms";
        URI    uri    = URI.create(urls.wsBase() + "/" + stream);

        java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> localBids =
            new java.util.concurrent.ConcurrentSkipListMap<>(java.util.Collections.reverseOrder());
        java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> localAsks =
            new java.util.concurrent.ConcurrentSkipListMap<>();

        Sinks.Many<OrderBook> sink = Sinks.many().multicast().onBackpressureBuffer();
        ReactorNettyWebSocketClient wsClient = new ReactorNettyWebSocketClient();

        return Flux.defer(() -> {
            wsClient.execute(uri, session ->
                session.receive()
                    .map(msg -> msg.getPayloadAsText())
                    .flatMap(json -> parseDepthUpdate(json, symbol, depth, localBids, localAsks))
                    .doOnNext(sink::tryEmitNext)
                    .then()
            ).subscribe();
            return sink.asFlux();
        })
        .retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
            .maxBackoff(Duration.ofSeconds(30))
            .doBeforeRetry(s -> { localBids.clear(); localAsks.clear(); }));
    }

    private Mono<OrderBook> parseDepthUpdate(
            String json, String symbol, int depth,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> bids,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> asks) {
        return Mono.fromCallable(() -> {
            JsonNode node = objectMapper.readTree(json);
            applyLevels(node.path("b"), bids, depth);
            applyLevels(node.path("a"), asks, depth);
            if (bids.isEmpty() || asks.isEmpty()) return null;
            return new OrderBook(symbol, bids,
                asks, node.path("u").asLong(0), Instant.now());
        }).onErrorResume(e -> Mono.empty()).filter(ob -> ob != null);
    }

    private void applyLevels(JsonNode levels,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> map, int depth) {
        if (!levels.isArray()) return;
        for (JsonNode level : levels) {
            BigDecimal price = new BigDecimal(level.get(0).asText());
            BigDecimal qty   = new BigDecimal(level.get(1).asText());
            if (qty.compareTo(BigDecimal.ZERO) == 0) map.remove(price);
            else                                       map.put(price, qty);
        }
        while (map.size() > depth) map.pollLastEntry();
    }

    // -------------------------------------------------------------------------
    // Account — GET /fapi/v2/account
    // -------------------------------------------------------------------------

    @Override
    public Mono<AccountState> fetchAccountState() {
        long   ts    = Instant.now().toEpochMilli();
        String query = "timestamp=" + ts;
        String sig   = HmacSigner.sign(secretKey, query);

        return webClient.get()
            .uri(urls.restBase() + urls.accountPath() + "?" + query + "&signature=" + sig)
            .header("X-MBX-APIKEY", apiKey)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(node -> {
                Map<String, BigDecimal> balances = new HashMap<>();
                // Futures account returns assets array with walletBalance
                for (JsonNode asset : node.path("assets")) {
                    BigDecimal walletBalance = new BigDecimal(asset.get("walletBalance").asText());
                    if (walletBalance.compareTo(BigDecimal.ZERO) > 0)
                        balances.put(asset.get("asset").asText(), walletBalance);
                }
                return AccountState.builder()
                    .balances(balances)
                    .openOrders(Map.of())
                    .snapshotTime(Instant.now())
                    .build();
            })
            .doOnError(e -> log.error("[{}] fetchAccountState failed: {}", marketName, e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Orders — POST /fapi/v1/order
    // -------------------------------------------------------------------------

    @Override
    public Mono<Order> submitOrder(Order order) {
        long   ts    = Instant.now().toEpochMilli();
        String query = buildOrderQuery(order, ts);
        String sig   = HmacSigner.sign(secretKey, query);

        return webClient.post()
            .uri(urls.restBase() + urls.orderPath() + "?" + query + "&signature=" + sig)
            .header("X-MBX-APIKEY", apiKey)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(node -> order.toBuilder()
                .exchangeOrderId(node.get("orderId").asText())
                .status(Order.Status.PENDING)
                .updatedAt(Instant.now())
                .build())
            .doOnNext(o  -> log.info("[{}] Order submitted: {} leverage={}x positionSide={}",
                marketName, o.clientOrderId(), order.leverage(), order.positionSide()))
            .doOnError(e -> log.error("[{}] submitOrder failed: {}", marketName, e.getMessage()));
    }

    @Override
    public Mono<Order> cancelOrder(String clientOrderId) {
        long   ts    = Instant.now().toEpochMilli();
        String query = "origClientOrderId=" + clientOrderId + "&timestamp=" + ts;
        String sig   = HmacSigner.sign(secretKey, query);

        return webClient.delete()
            .uri(urls.restBase() + urls.orderPath() + "?" + query + "&signature=" + sig)
            .header("X-MBX-APIKEY", apiKey)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(node -> Order.builder()
                .clientOrderId(clientOrderId)
                .exchangeOrderId(node.get("orderId").asText())
                .status(Order.Status.CANCELLED)
                .updatedAt(Instant.now())
                .build())
            .doOnError(e -> log.error("[{}] cancelOrder failed: {}", marketName, e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Futures-specific
    // -------------------------------------------------------------------------

    @Override
    public Mono<Void> setLeverage(String symbol, int leverage) {
        long   ts    = Instant.now().toEpochMilli();
        String query = "symbol=" + symbol + "&leverage=" + leverage + "&timestamp=" + ts;
        String sig   = HmacSigner.sign(secretKey, query);

        return webClient.post()
            .uri(urls.restBase() + "/fapi/v1/leverage?" + query + "&signature=" + sig)
            .header("X-MBX-APIKEY", apiKey)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .doOnNext(n -> log.info("[{}] Leverage set: {}x for {}", marketName, leverage, symbol))
            .doOnError(e -> log.error("[{}] setLeverage failed: {}", marketName, e.getMessage()))
            .then();
    }

    @Override
    public Mono<Void> setPositionMode(boolean hedgeMode) {
        long   ts    = Instant.now().toEpochMilli();
        String query = "dualSidePosition=" + hedgeMode + "&timestamp=" + ts;
        String sig   = HmacSigner.sign(secretKey, query);

        return webClient.post()
            .uri(urls.restBase() + "/fapi/v1/positionSide/dual?" + query + "&signature=" + sig)
            .header("X-MBX-APIKEY", apiKey)
            .retrieve()
            .bodyToMono(Void.class)
            .doOnSuccess(v -> log.info("[{}] Position mode set to: {}",
                marketName, hedgeMode ? "HEDGE" : "ONE-WAY"))
            .doOnError(e -> log.error("[{}] setPositionMode failed: {}", marketName, e.getMessage()));
    }

    @Override
    public Mono<Order> closePosition(String symbol, Order.PositionSide positionSide) {
        // Build a reduce-only market order in the opposite direction
        long   ts    = Instant.now().toEpochMilli();
        // Side is opposite to positionSide: LONG position → SELL to close
        Order.Side closeSide = positionSide == Order.PositionSide.LONG
            ? Order.Side.SELL : Order.Side.BUY;
        String query = "symbol=" + symbol
            + "&side=" + closeSide
            + "&type=MARKET"
            + "&positionSide=" + positionSide
            + "&reduceOnly=true"
            + "&timestamp=" + ts;
        String sig = HmacSigner.sign(secretKey, query);

        return webClient.post()
            .uri(urls.restBase() + urls.orderPath() + "?" + query + "&signature=" + sig)
            .header("X-MBX-APIKEY", apiKey)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(node -> Order.builder()
                .symbol(symbol).side(closeSide).type(Order.Type.MARKET)
                .positionSide(positionSide).reduceOnly(true)
                .exchangeOrderId(node.get("orderId").asText())
                .status(Order.Status.PENDING)
                .createdAt(Instant.now()).updatedAt(Instant.now())
                .build())
            .doOnNext(o -> log.info("[{}] Close position submitted: {} {}", marketName, closeSide, symbol))
            .doOnError(e -> log.error("[{}] closePosition failed: {}", marketName, e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String buildOrderQuery(Order order, long ts) {
        StringBuilder sb = new StringBuilder();
        sb.append("symbol=").append(order.symbol())
          .append("&side=").append(order.side())
          .append("&type=").append(order.type())
          .append("&quantity=").append(order.quantity().toPlainString())
          .append("&newClientOrderId=").append(order.clientOrderId())
          .append("&timestamp=").append(ts);
        if (order.positionSide() != null)
            sb.append("&positionSide=").append(order.positionSide());
        if (Boolean.TRUE.equals(order.reduceOnly()))
            sb.append("&reduceOnly=true");
        if (order.type() == Order.Type.LIMIT && order.limitPrice() != null)
            sb.append("&price=").append(order.limitPrice().toPlainString())
              .append("&timeInForce=GTC");
        return sb.toString();
    }
}
