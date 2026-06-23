package com.tradingbot.service;

import com.tradingbot.model.Candle;
import reactor.core.publisher.Flux;

/**
 * Интерфейс для получения живого потока подтвержденных (закрытых) свечей через WebSockets.
 */
public interface RealtimeCandleSource {

    /**
     * Подписывается на WebSocket-канал биржи и возвращает реактивный поток свечей для указанного тикера.
     * 
     * @param symbol Торговая пара (например, "BTC-USDT")
     * @return Поток подтвержденных свечей в реальном времени
     */
    Flux<Candle> subscribeToCandles(String symbol);
}
