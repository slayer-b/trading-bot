package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.strategy.helper.MarketStateTracker;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;

public class BreakoutRetestStrategy implements TradingStrategy {

    private static final BigDecimal RISK_PERCENTAGE = new BigDecimal("0.02"); // 2% от баланса
    private static final String QUOTE_ASSET = "USDT";

    public BreakoutRetestStrategy() {
        System.out.println("====================================================");
        System.out.println("[OK] BreakoutRetestStrategy успешно инициализирована!");
        System.out.println("====================================================");
    }

    @Override
    public String name() {
        return "breakout_retest_daily_15m_5m";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        System.out.println("[ДВИЖОК] Активирован сквозной анализ свечей для " + symbol);

        MarketStateTracker tracker = new MarketStateTracker();
        var processedTimestamps = ConcurrentHashMap.<LocalDateTime>newKeySet();

        return accountState
                .defaultIfEmpty(AccountState.builder().build())
                .flatMapMany(account -> candles
                        // Исключаем дубли на уровне реактивного потока
                        .filter(candle -> processedTimestamps.add(candle.openTime()))
                        .map(candle5m -> {
                            tracker.updateState(candle5m);

                            BigDecimal usdtBalance = account.balance(QUOTE_ASSET);
                            BigDecimal tradeAmount = usdtBalance.multiply(RISK_PERCENTAGE);
                            if (tradeAmount.compareTo(BigDecimal.ZERO) <= 0) {
                                tradeAmount = new BigDecimal("10"); // Минимальный резерв ордера
                            }

                            Signal signal = tracker.evaluateSignal(candle5m, symbol, tradeAmount);
                            if (signal.action() != Signal.Action.HOLD) {
                                System.out.println("[!!!] СФОРМИРОВАН ТОРГОВЫЙ СИГНАЛ: " + signal.action() + " | Цена: " + signal.limitPrice());
                            }
                            return signal;
                        })
                )
                .filter(signal -> signal.action() != Signal.Action.HOLD)
                .doFinally(signalType -> processedTimestamps.clear());
    }
}
