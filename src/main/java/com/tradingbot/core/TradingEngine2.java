package com.tradingbot.core;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.service.HistoricalCandleRepository;
import com.tradingbot.service.RealtimeCandleSource;
import com.tradingbot.strategy.TradingStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Торговый движок нового поколения с поддержкой реактивного разогрева (Warm-up).
 * Склеивает исторические данные для расчета уровней и живой поток с вебсокета биржи.
 */
public class TradingEngine2 {

//    private final TradingStrategy breakoutStrategy;
//    private final HistoricalCandleRepository historicalRepository; // Интерфейс вашей БД или REST-клиента биржи
//    private final RealtimeCandleSource realtimeSource;             // Стрим живых свечей с вебсокета
//
//    public TradingEngine2(TradingStrategy breakoutStrategy,
//                           HistoricalCandleRepository historicalRepository,
//                           RealtimeCandleSource realtimeSource) {
//        this.historicalRepository = historicalRepository;
//        this.realtimeSource = realtimeSource;
//    }
//
//    /**
//     * Запускает торговую сессию, предварительно прогревая стейт стратегии историей.
//     *
//     * @param accountState Моно-поток с актуальным состоянием баланса
//     * @param symbol       Торговая пара (например, "SOLUSDT")
//     * @return Поток только актуальных торговых сигналов в реальном времени
//     */
//    public Flux<Signal> startTradingSession(Mono<AccountState> accountState, String symbol) {
//        // 1. Запрашиваем исторические 5-минутные свечи за последние 48 часов для расчета уровней вчерашнего дня
//        LocalDateTime warmUpStart = LocalDateTime.now().minusDays(2);
//        Flux<Candle> historicalCandles = historicalRepository.findBySymbolAndAfter(symbol, warmUpStart);
//
//        // 2. Подключаемся к живому потоку свечей с вебсокета
//        Flux<Candle> realtimeCandles = realtimeSource.subscribeToCandles(symbol);
//
//        // Потокобезопасный сет для дедупликации свечей на стыке двух потоков
//        var processedTimestamps = ConcurrentHashMap.<LocalDateTime>newKeySet();
//
//        // 3. Склеиваем историю и вебсокет в единую хронологическую цепочку
//        Flux<Candle> combinedCandleStream = Flux.concat(historicalCandles, realtimeCandles)
//                // Защита: если база данных и вебсокет пересекутся по времени, исключаем дубли
//                .filter(candle -> processedTimestamps.add(candle.openTime()));
//
//        // Максимальный возраст сигнала для исполнения (30 секунд)
//        Instant executionThreshold = Instant.now().minusSeconds(30);
//
//        // 4. Передаем прогретый поток в стратегию и фильтруем старые сигналы
//        return breakoutStrategy.evaluate(combinedCandleStream, accountState, symbol)
//                // Защита: отсекаем сигналы, которые стратегия нашла глубоко в истории во время разогрева
//                .filter(signal -> signal.timestamp().isAfter(executionThreshold))
//                // Очищаем кэш таймстампов при закрытии или падении потока для экономии памяти
//                .doFinally(signalType -> processedTimestamps.clear());
//    }
}
