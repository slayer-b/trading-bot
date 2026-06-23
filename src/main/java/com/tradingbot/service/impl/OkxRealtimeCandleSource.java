package com.tradingbot.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.model.Candle;
import com.tradingbot.service.RealtimeCandleSource;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Service
public class OkxRealtimeCandleSource implements RealtimeCandleSource {

    private final WebSocketClient webSocketClient;
    private final ObjectMapper objectMapper;
    
    // Публичный WebSocket эндпоинт OKX v5 Business Channel
    private static final String OKX_WS_URL = "wss://://okx.com";

    public OkxRealtimeCandleSource(ObjectMapper objectMapper) {
        this.webSocketClient = new ReactorNettyWebSocketClient();
        this.objectMapper = objectMapper;
    }

    @Override
    public Flux<Candle> subscribeToCandles(String symbol) {
        // Приводим тикер к формату OKX (например, из BTCUSDT делаем BTC-USDT)
        String okxSymbol = formatToOkxSymbol(symbol);

        // JSON-запрос для подписки на канал 5-минутных свечей
        String subscribeJson = String.format(
                "{\"op\": \"subscribe\", \"args\": [{\"channel\": \"candles5m\", \"instId\": \"%s\"}]}",
                okxSymbol
        );

        return Flux.create(sink -> {
            webSocketClient.execute(URI.create(OKX_WS_URL), session -> {
                
                // Отправляем фрейм подписки на биржу
                Mono<Void> sendSubscribe = session.send(
                        Mono.just(session.textMessage(subscribeJson))
                );

                // Слушаем входящие сообщения
                Mono<Void> receiveStream = session.receive()
                        .map(WebSocketMessage::getPayloadAsText)
                        .flatMap(text -> {
                            try {
                                JsonNode root = objectMapper.readTree(text);
                                
                                // Пропускаем системные сообщения (например, подтверждение подписки event: "subscribe")
                                if (root.has("data")) {
                                    JsonNode dataNode = root.get("data");
                                    if (dataNode.isArray() && !dataNode.isEmpty()) {
                                        JsonNode candleArray = dataNode.get(0);
                                        
                                        // Важно: OKX шлет в реалтайме статус свечи в последнем поле.
                                        // "0" означает, что свеча еще формируется (тик).
                                        // "1" означает, что свеча официально закрылась.
                                        // Ваша стратегия требует СТРОГО подтвержденные (completed) свечи.
                                        String confirmStatus = candleArray.get(8).asText();
                                        
                                        if ("1".equals(confirmStatus)) {
                                            Candle candle = parseCandle(candleArray, symbol);
                                            sink.next(candle);
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                // Логируем ошибку парсинга, но не тушим весь стрим
                                System.err.println("Ошибка разбора WS кадра: " + e.getMessage());
                            }
                            return Mono.empty();
                        })
                        .then();

                // Объединяем отправку подписки и чтение потока
                return sendSubscribe.then(receiveStream);
            })
            // В случае критической ошибки вебсокета или дисконнекта передаем ошибку в поток для реконнекта
            .subscribe(
                    null,
                    sink::error,
                    sink::complete
            );
        });
    }

    private Candle parseCandle(JsonNode node, String originSymbol) {
        // Структура массива свечи в WS OKX: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        long timestamp = node.get(0).asLong();
        BigDecimal open  = new BigDecimal(node.get(1).asText());
        BigDecimal high  = new BigDecimal(node.get(2).asText());
        BigDecimal low   = new BigDecimal(node.get(3).asText());
        BigDecimal close = new BigDecimal(node.get(4).asText());
        BigDecimal volume = new BigDecimal(node.get(5).asText());

        LocalDateTime openTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
        LocalDateTime closeTime = openTime.plusMinutes(5);

        return Candle.builder()
                .symbol(originSymbol)
                .openTime(openTime)
                .closeTime(closeTime)
                .open(open)
                .high(high)
                .low(low)
                .close(close)
                .volume(volume)
                .tickCount(0) // Информации о тиках в базовом кадре нет
                .timeframe(Duration.ofMinutes(5))
                .build();
    }

    private String formatToOkxSymbol(String symbol) {
        if (symbol == null) return "";
        if (symbol.contains("-")) return symbol;
        
        // Если тикер пришел как BTCUSDT, превращаем его в BTC-USDT (поддерживает USDT, USDC, EUR)
        if (symbol.endsWith("USDT")) return symbol.replace("USDT", "-USDT");
        if (symbol.endsWith("USDC")) return symbol.replace("USDC", "-USDC");
        
        return symbol;
    }
}
