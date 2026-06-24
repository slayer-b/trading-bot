package com.tradingbot.service.impl;

import com.tradingbot.model.Candle;
import com.tradingbot.service.HistoricalCandleRepository;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Repository;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Repository
public class OkxHistoricalCandleRepository implements HistoricalCandleRepository {

    private final WebClient webClient;
//    private static final String OKX_API_URL = "https://www.okx.com";
    private static final String OKX_API_URL = "https://okcn.com";


    public OkxHistoricalCandleRepository(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.baseUrl(OKX_API_URL).build();
    }

    @Override
    public Flux<Candle> findBySymbolAndAfter(String symbol, LocalDateTime after) {
        long afterMs = after.toInstant(ZoneOffset.UTC).toEpochMilli();

        return webClient.get()
                // Используем эндпоинт истории закрытых свечей OKX v5
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v5/market/history-candles")
                        .queryParam("instId", symbol)
                        .queryParam("bar", "5m") // Наш базовый таймфрейм для стратегии
                        .queryParam("after", afterMs) // Фильтр "до какого времени" (в логике OKX pagination идет назад)
                        .queryParam("limit", "100")   // Максимум за один запрос (хватит для warm-up за 2-3 дня)
                        .build())
                .retrieve()
                // OKX возвращает структуру вида: { "code": "0", "msg": "", "data": [[...], [...]] }
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .flatMapMany(response -> {
                    String code = (String) response.get("code");
                    if (!"0".equals(code)) {
                        return Flux.error(new RuntimeException("OKX API Error: " + response.get("msg")));
                    }
                    
                    List<List<String>> data = (List<List<String>>) response.getOrDefault("data", Collections.emptyList());
                    
                    // OKX возвращает свечи от НОВЫХ к СТАРЫМ. 
                    // Для последовательного разогрева стейта нам нужно развернуть их хронологически (от СТАРЫХ к НОВЫМ).
                    return Flux.fromIterable(data)
                            .map(this::mapToCandle)
                            // Отфильтровываем только те, которые реально позже нашего warmUpStart (на всякий случай)
                            .filter(candle -> candle.openTime().isAfter(after))
                            // Сортируем: от старых к новым
                            .sort((c1, c2) -> c1.openTime().compareTo(c2.openTime()));
                });
    }

    private Candle mapToCandle(List<String> rawFields) {
        // Структура ответа OKX: [ts, open, high, low, close, vol, volCcy, qty, confirm]
        long timestamp = Long.parseLong(rawFields.get(0));
        BigDecimal open  = new BigDecimal(rawFields.get(1));
        BigDecimal high  = new BigDecimal(rawFields.get(2));
        BigDecimal low   = new BigDecimal(rawFields.get(3));
        BigDecimal close = new BigDecimal(rawFields.get(4));
        BigDecimal volume = new BigDecimal(rawFields.get(5)); // Объем в базовой валюте
        
        // В OKX поле tickCount явно не отдается в этом эндпоинте, ставим 0 или дефолт
        int tickCount = 0; 

        LocalDateTime openTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
        // Так как таймфрейм 5 минут, время закрытия — это время открытия + 5 минут
        LocalDateTime closeTime = openTime.plusMinutes(5);

        return Candle.builder()
                .symbol(rawFields.get(0)) // Временный маппинг, символ передадим снаружи или оставим из контекста
                .openTime(openTime)
                .closeTime(closeTime)
                .open(open)
                .high(high)
                .low(low)
                .close(close)
                .volume(volume)
                .tickCount(tickCount)
                .timeframe(Duration.ofMinutes(5))
                .build();
    }
}
