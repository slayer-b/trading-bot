package com.tradingbot.strategy; // Укажите ваш пакет

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Order;
import com.tradingbot.model.Signal;
import com.tradingbot.model.Tick;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class MacdStrategy2 implements TradingStrategy2 {

    // Классические параметры MACD (12, 26, 9)
    private static final int FAST_PERIOD = 12;
    private static final int SLOW_PERIOD = 26;
    private static final int SIGNAL_PERIOD = 9;

    private static final BigDecimal FAST_ALPHA = calculateAlpha(FAST_PERIOD);
    private static final BigDecimal SLOW_ALPHA = calculateAlpha(SLOW_PERIOD);
    private static final BigDecimal SIGNAL_ALPHA = calculateAlpha(SIGNAL_PERIOD);

    // Вспомогательные константы
    private static final BigDecimal TWO = BigDecimal.valueOf(2);
    private static final BigDecimal TRADE_USDT_AMOUNT = BigDecimal.valueOf(20); // Объем ордера в USDT

    // Атомарный контейнер состояния (Защита от утечки памяти OOM)
    private final AtomicReference<MacdState> state = new AtomicReference<>(new MacdState());

    @Override
    public String name() {
        return "MACD Strategy v2 (Tick-to-Candle)";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Tick> ticks, Mono<AccountState> accountState) {
        return ticks
                // 1. Получаем рабочую цену из тика
                .map(this::resolvePrice)

                // 2. Игнорируем дубли цены (оптимизация нагрузки на CPU)
                .distinctUntilChanged()

                // 3. Собираем тики в минутные окна (предотвращает раздувание RAM)
                .window(Duration.ofMinutes(1))
                .flatMap(priceWindow -> priceWindow
                        // Находим Close цену минутной свечи (последний элемент окна)
                        .reduce((first, second) -> second)
                )

                // 4. Обновляем итеративные EMA и ищем пересечения линий индикатора
                .map(this::updateMacdAndGetSignal)

                // 5. Пропускаем дальше только сформированные команды (BUY или SELL)
                .filter(action -> action != ActionType.HOLD)

                // 6. Синхронизируемся с AccountState, чтобы Reactor сохранял контекст
                // и подписка на баланс корректно обрабатывалась движком
                .withLatestFrom(accountState, (action, account) -> action)

                // 7. Собираем итоговый Signal, извлекая метаданные из исходного потока тиков
                .withLatestFrom(ticks, (action, currentTick) -> {
                    BigDecimal executionPrice = resolvePrice(currentTick);
                    boolean isBuy = action == ActionType.BUY;

                    return Signal.builder()
                            .action(isBuy ? Signal.Action.BUY : Signal.Action.SELL)
                            .symbol(currentTick.symbol()) // Для POJO измените на getSymbol()
                            .usdtAmount(TRADE_USDT_AMOUNT)
                            .limitPrice(executionPrice)
                            .reason("MACD Line crossed Signal Line. Action: " + action)
                            .timestamp(Instant.now())
                            .leverage(1)
                            .positionSide(isBuy ? Order.PositionSide.LONG : Order.PositionSide.SHORT)
                            .build();
                });
    }

    /**
     * Безопасное определение цены.
     * Берет lastPrice, если сделок нет — вычисляет средний Mid Price.
     */
    private BigDecimal resolvePrice(Tick tick) {
        if (tick.lastPrice() != null && tick.lastPrice().compareTo(BigDecimal.ZERO) > 0) {
            return tick.lastPrice();
        }
        if (tick.bid() != null && tick.ask() != null) {
            return tick.bid().add(tick.ask()).divide(TWO, 8, RoundingMode.HALF_UP);
        }
        return BigDecimal.ZERO;
    }

    /**
     * Математический расчет EMA "на лету" без сохранения истории в массивы.
     */
    private ActionType updateMacdAndGetSignal(BigDecimal closePrice) {
        if (closePrice.compareTo(BigDecimal.ZERO) == 0) {
            return ActionType.HOLD;
        }

        return state.updateAndGet(current -> {
            BigDecimal nextFastEma = current.fastEma == null ? closePrice : calculateEma(closePrice, current.fastEma, FAST_ALPHA);
            BigDecimal nextSlowEma = current.slowEma == null ? closePrice : calculateEma(closePrice, current.slowEma, SLOW_ALPHA);

            BigDecimal nextMacd = nextFastEma.subtract(nextSlowEma);
            BigDecimal nextSignalLine = current.signalLine == null ? nextMacd : calculateEma(nextMacd, current.signalLine, SIGNAL_ALPHA);

            ActionType triggeredAction = ActionType.HOLD;

            if (current.macd != null && current.signalLine != null) {
                boolean previouslyBelow = current.macd.compareTo(current.signalLine) <= 0;
                boolean nowAbove = nextMacd.compareTo(nextSignalLine) > 0;

                boolean previouslyAbove = current.macd.compareTo(current.signalLine) >= 0;
                boolean nowBelow = nextMacd.compareTo(nextSignalLine) < 0;

                if (previouslyBelow && nowAbove) {
                    triggeredAction = ActionType.BUY;
                } else if (previouslyAbove && nowBelow) {
                    triggeredAction = ActionType.SELL;
                }
            }

            return new MacdState(nextFastEma, nextSlowEma, nextMacd, nextSignalLine, triggeredAction);
        }).lastAction();
    }

    private static BigDecimal calculateAlpha(int period) {
        return BigDecimal.valueOf(2.0 / (period + 1));
    }

    private BigDecimal calculateEma(BigDecimal currentPrice, BigDecimal previousEma, BigDecimal alpha) {
        BigDecimal oneMinusAlpha = BigDecimal.ONE.subtract(alpha);
        return currentPrice.multiply(alpha)
                .add(previousEma.multiply(oneMinusAlpha))
                .setScale(8, RoundingMode.HALF_UP);
    }

    // Вспомогательные внутренние структуры
    private enum ActionType {BUY, SELL, HOLD}

    private record MacdState(
            BigDecimal fastEma,
            BigDecimal slowEma,
            BigDecimal macd,
            BigDecimal signalLine,
            ActionType lastAction
    ) {
        public MacdState() {
            this(null, null, null, null, ActionType.HOLD);
        }
    }
}
