package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Signal;
import com.tradingbot.model.Tick;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TradingStrategy2 {

    /**
     * Возвращает уникальное имя стратегии.
     */
    String name();

    /**
     * Основной метод вычисления сигналов на основе входящего потока тиков.
     *
     * @param ticks        Flux-поток входящих рыночных тиков (сделок) в реальном времени.
     * @param accountState Mono с актуальным состоянием баланса аккаунта.
     * @return Flux-поток сигналов BUY/SELL для торгового движка.
     */
    Flux<Signal> evaluate(Flux<Tick> ticks, Mono<AccountState> accountState);
}
