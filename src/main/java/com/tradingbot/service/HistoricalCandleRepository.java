package com.tradingbot.service;

import com.tradingbot.model.Candle;
import reactor.core.publisher.Flux;
import java.time.LocalDateTime;

/**
 * Интерфейс для получения исторических свечей из внешних источников (БД или API биржи).
 */
public interface HistoricalCandleRepository {
    
    /**
     * Возвращает поток свечей для указанного тикера, начиная с определенного времени.
     * Свечи должны идти в хронологическом порядке (от старых к новым).
     */
    Flux<Candle> findBySymbolAndAfter(String symbol, LocalDateTime after);
}