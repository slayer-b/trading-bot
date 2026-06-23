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

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Live Binance-protocol market (used for both Binance and BinanceAlpha).
 * All URLs come from {@link MarketUrls} — nothing is hardcoded.
 */
public class BinanceMarket implements Market {

    private static final Logger log = LoggerFactory.getLogger(BinanceMarket.class);

    private final WebClient    webClient;
    private final ObjectMapper objectMapper;
    private final MarketUrls   urls;
    private final String       marketName;
    private final String       apiKey;
    private final String       secretKey;
    private final BigDecimal   commissionRate;

    public BinanceMarket(WebClient webClient, ObjectMapper objectMapper,
                         MarketUrls urls, String marketName,
                         String apiKey, String secretKey, BigDecimal commissionRate) {
        this.webClient      = webClient;
        this.objectMapper   = objectMapper;
        this.urls           = urls;
        this.marketName     = marketName;
        this.apiKey         = apiKey;
        this.secretKey      = secretKey;
        this.commissionRate = commissionRate;
    }

    @Override
    public String name() { return marketName; }

    @Override
    public BigDecimal commissionRate() { return commissionRate; }

    // -------------------------------------------------------------------------
    // Tick stream
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
            .doBeforeRetry(sig ->
                log.warn("[{}] WS disconnected, retrying: {}", marketName, sig.failure().getMessage())));
    }

    private Mono<Tick> parseTick(String json, String symbol) {
        return Mono.fromCallable(() -> {
            JsonNode node = objectMapper.readTree(json);
            // Binance bookTicker: b=best bid, a=best ask
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
    // Order book stream — Binance @depth<N>@100ms incremental updates
    // -------------------------------------------------------------------------

    @Override
    public Flux<OrderBook> orderBookStream(String symbol, int depth) {
        // Use the partial book depth stream: pushes top-N levels every 100ms
        String stream = symbol.toLowerCase() + "@depth" + depth + "@100ms";
        URI    uri    = URI.create(urls.wsBase() + "/" + stream);

        // Local book maintained incrementally
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
                    .flatMap(json -> parseDepthUpdate(json, symbol, depth,
                                                       localBids, localAsks))
                    .doOnNext(sink::tryEmitNext)
                    .then()
            ).subscribe();
            return sink.asFlux();
        })
        .retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
            .maxBackoff(Duration.ofSeconds(30))
            .doBeforeRetry(s -> {
                localBids.clear();
                localAsks.clear();
                log.warn("[{}] Order book WS disconnected, retrying: {}",
                    marketName, s.failure().getMessage());
            }));
    }

    private Mono<OrderBook> parseDepthUpdate(
            String json, String symbol, int depth,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> bids,
            java.util.concurrent.ConcurrentSkipListMap<BigDecimal, BigDecimal> asks) {

        return Mono.fromCallable(() -> {
            JsonNode node = objectMapper.readTree(json);

            // Apply bid updates: quantity=0 means remove the level
            JsonNode bidsNode = node.path("b");
            if (bidsNode.isArray()) {
                for (JsonNode level : bidsNode) {
                    BigDecimal price = new BigDecimal(level.get(0).asText());
                    BigDecimal qty   = new BigDecimal(level.get(1).asText());
                    if (qty.compareTo(BigDecimal.ZERO) == 0) bids.remove(price);
                    else                                       bids.put(price, qty);
                }
            }

            // Apply ask updates
            JsonNode asksNode = node.path("a");
            if (asksNode.isArray()) {
                for (JsonNode level : asksNode) {
                    BigDecimal price = new BigDecimal(level.get(0).asText());
                    BigDecimal qty   = new BigDecimal(level.get(1).asText());
                    if (qty.compareTo(BigDecimal.ZERO) == 0) asks.remove(price);
                    else                                       asks.put(price, qty);
                }
            }

            // Trim to requested depth
            trimToDepth(bids, depth);
            trimToDepth(asks, depth);

            if (bids.isEmpty() || asks.isEmpty()) return null;

            return new OrderBook(symbol,
                bids,
                asks,
                node.path("u").asLong(0),
                Instant.now());
        }).onErrorResume(e -> {
            log.error("[{}] Failed to parse depth update: {}", marketName, e.getMessage());
            return Mono.empty();
        }).filter(ob -> ob != null);
    }

    private void trimToDepth(java.util.NavigableMap<BigDecimal, BigDecimal> map, int depth) {
        while (map.size() > depth) {
            map.pollLastEntry();
        }
    }

    // -------------------------------------------------------------------------
    // Account state
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
                for (JsonNode b : node.get("balances")) {
                    BigDecimal free = new BigDecimal(b.get("free").asText());
                    if (free.compareTo(BigDecimal.ZERO) > 0)
                        balances.put(b.get("asset").asText(), free);
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
    // Order submission
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
            .doOnNext(o  -> log.info("[{}] Order submitted: {}", marketName, o.clientOrderId()))
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
            .doOnNext(o  -> log.info("[{}] Order cancelled: {}", marketName, clientOrderId))
            .doOnError(e -> log.error("[{}] cancelOrder failed: {}", marketName, e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String buildOrderQuery(Order order, long timestamp) {
        StringBuilder sb = new StringBuilder();
        sb.append("symbol=").append(order.symbol())
          .append("&side=").append(order.side())
          .append("&type=").append(order.type())
          .append("&quantity=").append(order.quantity().toPlainString())
          .append("&newClientOrderId=").append(order.clientOrderId())
          .append("&timestamp=").append(timestamp);
        if (order.type() == Order.Type.LIMIT && order.limitPrice() != null) {
            sb.append("&price=").append(order.limitPrice().toPlainString())
              .append("&timeInForce=GTC");
        }
        return sb.toString();
    }
}
