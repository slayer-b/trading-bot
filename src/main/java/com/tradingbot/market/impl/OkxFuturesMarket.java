package com.tradingbot.market.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.market.FuturesMarket;
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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * OKX USDT-M Perpetual Swap futures market.
 *
 * <p>Uses OKX swap instrument type: instType=SWAP, e.g. BTC-USDT-SWAP.
 *
 * <p>Supports:
 * <ul>
 *   <li>One-way mode  — {@code posSide = net}</li>
 *   <li>Hedge mode    — {@code posSide = long | short}</li>
 *   <li>Per-order leverage via {@link #setLeverage}</li>
 * </ul>
 */
public class OkxFuturesMarket implements FuturesMarket {

    private static final Logger log = LoggerFactory.getLogger(OkxFuturesMarket.class);
    private static final DateTimeFormatter ISO_UTC =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final WebClient    webClient;
    private final ObjectMapper objectMapper;
    private final MarketUrls   urls;
    private final String       apiKey;
    private final String       secretKey;
    private final String       passphrase;
    private final BigDecimal   commissionRate;

    public OkxFuturesMarket(WebClient webClient, ObjectMapper objectMapper,
                             MarketUrls urls,
                             String apiKey, String secretKey, String passphrase,
                             BigDecimal commissionRate) {
        this.webClient      = webClient;
        this.objectMapper   = objectMapper;
        this.urls           = urls;
        this.apiKey         = apiKey;
        this.secretKey      = secretKey;
        this.passphrase     = passphrase;
        this.commissionRate = commissionRate;
    }

    @Override public String     name()           { return "OKX Futures"; }
    @Override public BigDecimal commissionRate() { return commissionRate; }

    // -------------------------------------------------------------------------
    // Tick stream — OKX public WS tickers channel
    // -------------------------------------------------------------------------

    @Override
    public Flux<Tick> tickStream(String symbol) {
        String instId   = toSwapSymbol(symbol);
        URI    uri      = URI.create(urls.wsBase());
        String subMsg   = """
            {"op":"subscribe","args":[{"channel":"tickers","instId":"%s"}]}
            """.formatted(instId).strip();

        Sinks.Many<Tick> sink = Sinks.many().multicast().onBackpressureBuffer();
        ReactorNettyWebSocketClient wsClient = new ReactorNettyWebSocketClient();

        return Flux.defer(() -> {
            wsClient.execute(uri, session ->
                session.send(Mono.just(session.textMessage(subMsg)))
                    .thenMany(session.receive()
                        .map(msg -> msg.getPayloadAsText())
                        .filter(json -> json.contains("\"tickers\""))
                        .flatMap(json -> parseTick(json, symbol)))
                    .doOnNext(sink::tryEmitNext)
                    .then()
            ).subscribe();
            return sink.asFlux();
        })
        .retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
            .maxBackoff(Duration.ofSeconds(30))
            .doBeforeRetry(s ->
                log.warn("[OKX Futures] WS disconnected, retrying: {}", s.failure().getMessage())));
    }

    private Mono<Tick> parseTick(String json, String symbol) {
        return Mono.fromCallable(() -> {
            JsonNode root = objectMapper.readTree(json);
            JsonNode data = root.path("data");
            if (!data.isArray() || data.isEmpty()) return null;
            JsonNode d = data.get(0);
            return Tick.builder()
                .symbol(symbol)
                .bid(new BigDecimal(d.get("bidPx").asText()))
                .ask(new BigDecimal(d.get("askPx").asText()))
                .lastPrice(new BigDecimal(d.get("last").asText()))
                .volume(new BigDecimal(d.get("vol24h").asText()))
                .timestamp(Instant.now())
                .build();
        }).onErrorResume(e -> {
            log.error("[OKX Futures] Tick parse error: {}", e.getMessage());
            return Mono.empty();
        }).filter(t -> t != null);
    }

    // -------------------------------------------------------------------------
    // Order book stream — OKX swap books channel
    // -------------------------------------------------------------------------

    @Override
    public Flux<OrderBook> orderBookStream(String symbol, int depth) {
        String instId  = toSwapSymbol(symbol);
        String channel = depth <= 5 ? "books5" : "books";
        URI    uri     = URI.create(urls.wsBase());
        String subMsg  = """
            {"op":"subscribe","args":[{"channel":"%s","instId":"%s"}]}
            """.formatted(channel, instId).strip();

        Sinks.Many<OrderBook> sink = Sinks.many().multicast().onBackpressureBuffer();
        ReactorNettyWebSocketClient wsClient = new ReactorNettyWebSocketClient();

        java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> localBids =
            new java.util.concurrent.ConcurrentSkipListMap<>(java.util.Collections.reverseOrder());
        java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> localAsks =
            new java.util.concurrent.ConcurrentSkipListMap<>();

        return Flux.defer(() -> {
            wsClient.execute(uri, session ->
                session.send(Mono.just(session.textMessage(subMsg)))
                    .thenMany(session.receive()
                        .map(msg -> msg.getPayloadAsText())
                        .filter(json -> json.contains("\"books"))
                        .flatMap(json -> parseBook(json, symbol, depth, localBids, localAsks)))
                    .doOnNext(sink::tryEmitNext)
                    .then()
            ).subscribe();
            return sink.asFlux();
        })
        .retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
            .maxBackoff(Duration.ofSeconds(30))
            .doBeforeRetry(s -> { localBids.clear(); localAsks.clear(); }));
    }

    private Mono<OrderBook> parseBook(
            String json, String symbol, int depth,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> bids,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> asks) {
        return Mono.fromCallable(() -> {
            JsonNode root = objectMapper.readTree(json);
            JsonNode data = root.path("data");
            if (!data.isArray() || data.isEmpty()) return null;
            JsonNode d = data.get(0);
            if ("snapshot".equals(root.path("action").asText())) { bids.clear(); asks.clear(); }
            applyLevels(d.path("bids"), bids, depth);
            applyLevels(d.path("asks"), asks, depth);
            if (bids.isEmpty() || asks.isEmpty()) return null;
            return new OrderBook(symbol, bids,
                asks, 0, Instant.now());
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
    // Account — GET /api/v5/account/balance
    // -------------------------------------------------------------------------

    @Override
    public Mono<AccountState> fetchAccountState() {
        String path      = urls.accountPath();
        String timestamp = ISO_UTC.format(Instant.now());
        String sig       = sign(timestamp, "GET", path, "");

        return webClient.get()
            .uri(urls.restBase() + path)
            .headers(h -> okxHeaders(h, timestamp, sig))
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(root -> {
                Map<String, BigDecimal> balances = new HashMap<>();
                for (JsonNode item : root.path("data").path(0).path("details")) {
                    BigDecimal avail = new BigDecimal(item.get("availBal").asText());
                    if (avail.compareTo(BigDecimal.ZERO) > 0)
                        balances.put(item.get("ccy").asText(), avail);
                }
                return AccountState.builder()
                    .balances(balances).openOrders(Map.of())
                    .snapshotTime(Instant.now()).build();
            })
            .doOnError(e -> log.error("[OKX Futures] fetchAccountState failed: {}", e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Orders — POST /api/v5/trade/order
    // -------------------------------------------------------------------------

    @Override
    public Mono<Order> submitOrder(Order order) {
        String path      = urls.orderPath();
        String timestamp = ISO_UTC.format(Instant.now());
        String body      = buildOrderBody(order);
        String sig       = sign(timestamp, "POST", path, body);

        return webClient.post()
            .uri(urls.restBase() + path)
            .headers(h -> { okxHeaders(h, timestamp, sig); h.set("Content-Type", "application/json"); })
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(root -> order.toBuilder()
                .exchangeOrderId(root.path("data").path(0).get("ordId").asText())
                .status(Order.Status.PENDING)
                .updatedAt(Instant.now())
                .build())
            .doOnNext(o  -> log.info("[OKX Futures] Order submitted: {} leverage={}x posSide={}",
                o.clientOrderId(), order.leverage(), order.positionSide()))
            .doOnError(e -> log.error("[OKX Futures] submitOrder failed: {}", e.getMessage()));
    }

    @Override
    public Mono<Order> cancelOrder(String clientOrderId) {
        String path      = urls.orderPath() + "/cancel";
        String timestamp = ISO_UTC.format(Instant.now());
        String body      = "{\"clOrdId\":\"%s\"}".formatted(clientOrderId);
        String sig       = sign(timestamp, "POST", path, body);

        return webClient.post()
            .uri(urls.restBase() + path)
            .headers(h -> { okxHeaders(h, timestamp, sig); h.set("Content-Type", "application/json"); })
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(root -> Order.builder()
                .clientOrderId(clientOrderId)
                .exchangeOrderId(root.path("data").path(0).path("ordId").asText())
                .status(Order.Status.CANCELLED)
                .updatedAt(Instant.now()).build())
            .doOnError(e -> log.error("[OKX Futures] cancelOrder failed: {}", e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Futures-specific
    // -------------------------------------------------------------------------

    @Override
    public Mono<Void> setLeverage(String symbol, int leverage) {
        String path      = "/api/v5/account/set-leverage";
        String timestamp = ISO_UTC.format(Instant.now());
        String body      = """
            {"instId":"%s","lever":"%d","mgnMode":"cross"}
            """.formatted(toSwapSymbol(symbol), leverage).strip();
        String sig = sign(timestamp, "POST", path, body);

        return webClient.post()
            .uri(urls.restBase() + path)
            .headers(h -> { okxHeaders(h, timestamp, sig); h.set("Content-Type", "application/json"); })
            .bodyValue(body)
            .retrieve()
            .bodyToMono(Void.class)
            .doOnSuccess(v -> log.info("[OKX Futures] Leverage set: {}x for {}", leverage, symbol))
            .doOnError(e  -> log.error("[OKX Futures] setLeverage failed: {}", e.getMessage()));
    }

    @Override
    public Mono<Void> setPositionMode(boolean hedgeMode) {
        String path      = "/api/v5/account/set-position-mode";
        String timestamp = ISO_UTC.format(Instant.now());
        String posMode   = hedgeMode ? "long_short_mode" : "net_mode";
        String body      = "{\"posMode\":\"%s\"}".formatted(posMode);
        String sig       = sign(timestamp, "POST", path, body);

        return webClient.post()
            .uri(urls.restBase() + path)
            .headers(h -> { okxHeaders(h, timestamp, sig); h.set("Content-Type", "application/json"); })
            .bodyValue(body)
            .retrieve()
            .bodyToMono(Void.class)
            .doOnSuccess(v -> log.info("[OKX Futures] Position mode: {}", posMode))
            .doOnError(e  -> log.error("[OKX Futures] setPositionMode failed: {}", e.getMessage()));
    }

    @Override
    public Mono<Order> closePosition(String symbol, Order.PositionSide positionSide) {
        String path      = urls.orderPath();
        String timestamp = ISO_UTC.format(Instant.now());
        // Opposite side to close: LONG → sell, SHORT → buy
        String side = positionSide == Order.PositionSide.LONG ? "sell" : "buy";
        String posSide = switch (positionSide) {
            case LONG  -> "long";
            case SHORT -> "short";
            default    -> "net";
        };
        String body = """
            {"instId":"%s","tdMode":"cross","side":"%s","posSide":"%s",
             "ordType":"market","sz":"0","reduceOnly":true}
            """.formatted(toSwapSymbol(symbol), side, posSide).strip();
        String sig = sign(timestamp, "POST", path, body);

        return webClient.post()
            .uri(urls.restBase() + path)
            .headers(h -> { okxHeaders(h, timestamp, sig); h.set("Content-Type", "application/json"); })
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(root -> Order.builder()
                .symbol(symbol)
                .side(side.equals("sell") ? Order.Side.SELL : Order.Side.BUY)
                .type(Order.Type.MARKET)
                .positionSide(positionSide).reduceOnly(true)
                .exchangeOrderId(root.path("data").path(0).path("ordId").asText())
                .status(Order.Status.PENDING)
                .createdAt(Instant.now()).updatedAt(Instant.now()).build())
            .doOnNext(o -> log.info("[OKX Futures] Close position submitted: {} {}", side, symbol))
            .doOnError(e -> log.error("[OKX Futures] closePosition failed: {}", e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** "BTCUSDT" → "BTC-USDT-SWAP" */
    private String toSwapSymbol(String symbol) {
        if (symbol.contains("-")) return symbol.endsWith("-SWAP") ? symbol : symbol + "-SWAP";
        if (symbol.endsWith("USDT")) return symbol.replace("USDT", "-USDT-SWAP");
        return symbol;
    }

    private String buildOrderBody(Order order) {
        String ordType = order.type() == Order.Type.LIMIT ? "limit" : "market";
        String posSide = order.positionSide() != null
            ? switch (order.positionSide()) {
                case LONG  -> "long";
                case SHORT -> "short";
                default    -> "net";
              }
            : "net";
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"instId\":\"").append(toSwapSymbol(order.symbol())).append("\",");
        sb.append("\"tdMode\":\"cross\",");
        sb.append("\"side\":\"").append(order.side().name().toLowerCase()).append("\",");
        sb.append("\"posSide\":\"").append(posSide).append("\",");
        sb.append("\"ordType\":\"").append(ordType).append("\",");
        sb.append("\"sz\":\"").append(order.quantity().toPlainString()).append("\",");
        sb.append("\"clOrdId\":\"").append(order.clientOrderId()).append("\"");
        if (order.type() == Order.Type.LIMIT && order.limitPrice() != null)
            sb.append(",\"px\":\"").append(order.limitPrice().toPlainString()).append("\"");
        if (Boolean.TRUE.equals(order.reduceOnly()))
            sb.append(",\"reduceOnly\":true");
        sb.append("}");
        return sb.toString();
    }

    private void okxHeaders(org.springframework.http.HttpHeaders h,
                             String timestamp, String sig) {
        h.set("OK-ACCESS-KEY",        apiKey);
        h.set("OK-ACCESS-SIGN",       sig);
        h.set("OK-ACCESS-TIMESTAMP",  timestamp);
        h.set("OK-ACCESS-PASSPHRASE", passphrase);
    }

    private String sign(String timestamp, String method, String path, String body) {
        try {
            String prehash = timestamp + method.toUpperCase() + path + body;
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return Base64.getEncoder().encodeToString(
                mac.doFinal(prehash.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("OKX signing failed", e);
        }
    }
}
