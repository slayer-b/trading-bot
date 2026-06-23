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
 * Live OKX market implementation.
 *
 * <p>Ticks: public WebSocket {@code /ws/v5/public}, subscribing to
 * {@code tickers} channel for the instrument.
 *
 * <p>Account + orders: private REST {@code /api/v5/account/balance} and
 * {@code /api/v5/trade/order}, signed with HMAC-SHA256 + Base64.
 *
 * <p>All URLs come from {@link MarketUrls} — nothing is hardcoded.
 */
public class OkxMarket implements Market {

    private static final Logger log = LoggerFactory.getLogger(OkxMarket.class);
    private static final DateTimeFormatter ISO_UTC =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final WebClient    webClient;
    private final ObjectMapper objectMapper;
    private final MarketUrls   urls;
    private final String       apiKey;
    private final String       secretKey;
    private final String       passphrase;
    private final BigDecimal   commissionRate;

    public OkxMarket(WebClient webClient, ObjectMapper objectMapper,
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

    @Override
    public String name() { return "OKX"; }

    @Override
    public BigDecimal commissionRate() { return commissionRate; }

    // -------------------------------------------------------------------------
    // Tick stream — OKX public WS, tickers channel
    // -------------------------------------------------------------------------

    @Override
    public Flux<Tick> tickStream(String symbol) {
        // OKX uses instrument ids like "BTC-USDT", not "BTCUSDT"
        String instId = toOkxSymbol(symbol);
        URI uri = URI.create(urls.wsBase() + urls.tickPath().replace("{symbol}", instId));
        Sinks.Many<Tick> sink = Sinks.many().multicast().onBackpressureBuffer();
        ReactorNettyWebSocketClient wsClient = new ReactorNettyWebSocketClient();

        // Subscribe message sent after WS connects
        String subscribeMsg = """
            {"op":"subscribe","args":[{"channel":"tickers","instId":"%s"}]}
            """.formatted(instId).strip();

        return Flux.defer(() -> {
            wsClient.execute(uri, session ->
                session.send(Mono.just(session.textMessage(subscribeMsg)))
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
            .doBeforeRetry(sig ->
                log.warn("[OKX] WS disconnected, retrying: {}", sig.failure().getMessage())));
    }

    private Mono<Tick> parseTick(String json, String symbol) {
        return Mono.fromCallable(() -> {
            JsonNode root = objectMapper.readTree(json);
            JsonNode data = root.path("data");
            if (!data.isArray() || data.isEmpty()) return null;
            JsonNode d = data.get(0);
            // OKX tickers fields: bidPx, askPx, last, vol24h
            return Tick.builder()
                .symbol(symbol)
                .bid(new BigDecimal(d.get("bidPx").asText()))
                .ask(new BigDecimal(d.get("askPx").asText()))
                .lastPrice(new BigDecimal(d.get("last").asText()))
                .volume(new BigDecimal(d.get("vol24h").asText()))
                .timestamp(Instant.now())
                .build();
        }).onErrorResume(e -> {
            log.error("[OKX] Failed to parse tick: {}", e.getMessage());
            return Mono.empty();
        }).filter(t -> t != null);
    }

    // -------------------------------------------------------------------------
    // Order book stream — OKX books channel
    // -------------------------------------------------------------------------

    @Override
    public Flux<OrderBook> orderBookStream(String symbol, int depth) {
        String instId    = toOkxSymbol(symbol);
        String channel   = depth <= 5 ? "books5" : depth <= 50 ? "books" : "books-l2-tbt";
        URI    uri       = URI.create(urls.wsBase());
        String subMsg    = """
            {"op":"subscribe","args":[{"channel":"%s","instId":"%s"}]}
            """.formatted(channel, instId).strip();

        Sinks.Many<OrderBook> sink = Sinks.many().multicast().onBackpressureBuffer();
        ReactorNettyWebSocketClient wsClient = new ReactorNettyWebSocketClient();

        // Local book state
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
                        .flatMap(json -> parseOkxBook(json, symbol, depth, localBids, localAsks)))
                    .doOnNext(sink::tryEmitNext)
                    .then()
            ).subscribe();
            return sink.asFlux();
        })
        .retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
            .maxBackoff(Duration.ofSeconds(30))
            .doBeforeRetry(s -> { localBids.clear(); localAsks.clear(); }));
    }

    private Mono<OrderBook> parseOkxBook(
            String json, String symbol, int depth,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> bids,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> asks) {
        return Mono.fromCallable(() -> {
            JsonNode root = objectMapper.readTree(json);
            JsonNode data = root.path("data");
            if (!data.isArray() || data.isEmpty()) return null;
            JsonNode d = data.get(0);

            // "snapshot" replaces entire book; "update" applies diffs
            String action = root.path("action").asText("snapshot");
            if ("snapshot".equals(action)) { bids.clear(); asks.clear(); }

            applyOkxLevels(d.path("bids"), bids, depth);
            applyOkxLevels(d.path("asks"), asks, depth);
            if (bids.isEmpty() || asks.isEmpty()) return null;

            return new OrderBook(symbol, bids,
                asks, d.path("seqId").asLong(0), Instant.now());
        }).onErrorResume(e -> {
            log.error("[OKX] Order book parse error: {}", e.getMessage());
            return Mono.empty();
        }).filter(ob -> ob != null);
    }

    private void applyOkxLevels(JsonNode levels,
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
    // Account state — GET /api/v5/account/balance
    // -------------------------------------------------------------------------

    @Override
    public Mono<AccountState> fetchAccountState() {
        String path      = urls.accountPath();
        String timestamp = ISO_UTC.format(Instant.now());
        String signature = sign(timestamp, "GET", path, "");

        return webClient.get()
            .uri(urls.restBase() + path)
            .header("OK-ACCESS-KEY",        apiKey)
            .header("OK-ACCESS-SIGN",       signature)
            .header("OK-ACCESS-TIMESTAMP",  timestamp)
            .header("OK-ACCESS-PASSPHRASE", passphrase)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(root -> {
                Map<String, BigDecimal> balances = new HashMap<>();
                JsonNode details = root.path("data").path(0).path("details");
                for (JsonNode item : details) {
                    BigDecimal avail = new BigDecimal(item.get("availBal").asText());
                    if (avail.compareTo(BigDecimal.ZERO) > 0)
                        balances.put(item.get("ccy").asText(), avail);
                }
                return AccountState.builder()
                    .balances(balances)
                    .openOrders(Map.of())
                    .snapshotTime(Instant.now())
                    .build();
            })
            .doOnError(e -> log.error("[OKX] fetchAccountState failed: {}", e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Order submission — POST /api/v5/trade/order
    // -------------------------------------------------------------------------

    @Override
    public Mono<Order> submitOrder(Order order) {
        String path      = urls.orderPath();
        String timestamp = ISO_UTC.format(Instant.now());
        String body      = buildOrderBody(order);
        String signature = sign(timestamp, "POST", path, body);

        return webClient.post()
            .uri(urls.restBase() + path)
            .header("OK-ACCESS-KEY",        apiKey)
            .header("OK-ACCESS-SIGN",       signature)
            .header("OK-ACCESS-TIMESTAMP",  timestamp)
            .header("OK-ACCESS-PASSPHRASE", passphrase)
            .header("Content-Type",         "application/json")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(root -> {
                JsonNode data = root.path("data").path(0);
                return order.toBuilder()
                    .exchangeOrderId(data.get("ordId").asText())
                    .status(Order.Status.PENDING)
                    .updatedAt(Instant.now())
                    .build();
            })
            .doOnNext(o  -> log.info("[OKX] Order submitted: {}", o.clientOrderId()))
            .doOnError(e -> log.error("[OKX] submitOrder failed: {}", e.getMessage()));
    }

    @Override
    public Mono<Order> cancelOrder(String clientOrderId) {
        String path      = urls.orderPath() + "/cancel";
        String timestamp = ISO_UTC.format(Instant.now());
        String body      = "{\"clOrdId\":\"%s\"}".formatted(clientOrderId);
        String signature = sign(timestamp, "POST", path, body);

        return webClient.post()
            .uri(urls.restBase() + path)
            .header("OK-ACCESS-KEY",        apiKey)
            .header("OK-ACCESS-SIGN",       signature)
            .header("OK-ACCESS-TIMESTAMP",  timestamp)
            .header("OK-ACCESS-PASSPHRASE", passphrase)
            .header("Content-Type",         "application/json")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(root -> Order.builder()
                .clientOrderId(clientOrderId)
                .exchangeOrderId(root.path("data").path(0).path("ordId").asText())
                .status(Order.Status.CANCELLED)
                .updatedAt(Instant.now())
                .build())
            .doOnNext(o  -> log.info("[OKX] Order cancelled: {}", clientOrderId))
            .doOnError(e -> log.error("[OKX] cancelOrder failed: {}", e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Convert "BTCUSDT" → "BTC-USDT" for OKX. */
    private String toOkxSymbol(String symbol) {
        if (symbol.contains("-")) return symbol; // already OKX format
        if (symbol.endsWith("USDT")) return symbol.replace("USDT", "-USDT");
        if (symbol.endsWith("BTC"))  return symbol.replace("BTC",  "-BTC");
        if (symbol.endsWith("ETH"))  return symbol.replace("ETH",  "-ETH");
        return symbol; // fallback — pass as-is
    }

    private String buildOrderBody(Order order) {
        String ordType = order.type() == Order.Type.LIMIT ? "limit" : "market";
        String side    = order.side().name().toLowerCase();
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"instId\":\"").append(toOkxSymbol(order.symbol())).append("\",");
        sb.append("\"tdMode\":\"cash\",");
        sb.append("\"side\":\"").append(side).append("\",");
        sb.append("\"ordType\":\"").append(ordType).append("\",");
        sb.append("\"sz\":\"").append(order.quantity().toPlainString()).append("\",");
        sb.append("\"clOrdId\":\"").append(order.clientOrderId()).append("\"");
        if (order.type() == Order.Type.LIMIT && order.limitPrice() != null)
            sb.append(",\"px\":\"").append(order.limitPrice().toPlainString()).append("\"");
        sb.append("}");
        return sb.toString();
    }

    /**
     * OKX signature: Base64( HMAC-SHA256( timestamp + method + requestPath + body ) )
     */
    private String sign(String timestamp, String method, String path, String body) {
        try {
            String prehash = timestamp + method.toUpperCase() + path + body;
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] hash = mac.doFinal(prehash.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new IllegalStateException("OKX signing failed", e);
        }
    }
}
