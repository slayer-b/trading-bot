package com.tradingbot.service;

import com.tradingbot.model.Candle;
import reactor.core.publisher.Flux;

/**
 * Infrastructure service contract for fetching historical market data.
 */
public interface HistoryService {

    /**
     * Identifies which market this history provider belongs to (e.g., "okx", "binance").
     */
    String marketName();

    /**
     * Fetches a historical stream of 5m candles for the specified asset pair.
     */
    Flux<Candle> fetchHistory(String symbol, int days);
}
