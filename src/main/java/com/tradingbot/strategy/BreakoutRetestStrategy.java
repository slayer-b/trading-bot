package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Order;
import com.tradingbot.model.Signal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BreakoutRetestStrategy implements TradingStrategy {

    private static final BigDecimal TWO = new BigDecimal("2");
    private static final BigDecimal ZERO_POINT_TWO = new BigDecimal("0.2");
    private static final BigDecimal RISK_PERCENTAGE = new BigDecimal("0.02");
    private static final String QUOTE_ASSET = "USDT";

    @Override
    public String name() {
        return "breakout_retest_daily_15m_5m";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        MarketStateTracker tracker = new MarketStateTracker();

        return accountState
                .defaultIfEmpty(AccountState.builder().build())
                .flatMapMany(account -> candles
                        .map(candle5m -> {
                            tracker.updateState(candle5m);

                            BigDecimal usdtBalance = account.balance(QUOTE_ASSET);
                            BigDecimal tradeAmount = usdtBalance.multiply(RISK_PERCENTAGE);
                            if (tradeAmount.compareTo(BigDecimal.ZERO) <= 0) {
                                tradeAmount = new BigDecimal("10"); // Минимальный резерв для тестов
                            }

                            return tracker.evaluateSignal(candle5m, symbol, tradeAmount);
                        })
                )
                .filter(signal -> signal.action() != Signal.Action.HOLD);
    }

    private static class MarketStateTracker {
        // Хранилище только ПОЛНОСТЬЮ закрытых исторических дней
        private final Map<LocalDate, BigDecimal[]> historicalDailyRanges = new HashMap<>();

        // Переменные для агрегации текущего (еще не закрытого) дня
        private LocalDate currentTrackingDay = null;
        private BigDecimal currentDayHigh = null;
        private BigDecimal currentDayLow = null;

        private final List<Candle> candles5m = new ArrayList<>();
        private final List<Candle> buffer15m = new ArrayList<>();

        // Состояние пробоя
        private BigDecimal yesterdayHigh = null;
        private BigDecimal yesterdayLow = null;
        private boolean isBreakoutConfirmed = false;
        private Order.PositionSide breakoutSide = null;
        private java.time.LocalDateTime breakoutConfirmedAt = null;
        private BigDecimal breakoutLevel = null;

        public void updateState(Candle candle5m) {
            LocalDate candleDay = candle5m.openTime().toLocalDate();

            // Если наступил новый день, закрываем старый и переносим его экстремумы в историю
            if (currentTrackingDay == null) {
                currentTrackingDay = candleDay;
                currentDayHigh = candle5m.high();
                currentDayLow = candle5m.low();
            } else if (!candleDay.equals(currentTrackingDay)) {
                // Фиксируем закрытый день в истории
                historicalDailyRanges.put(currentTrackingDay, new BigDecimal[]{currentDayHigh, currentDayLow});

                // Инициализируем новый день
                currentTrackingDay = candleDay;
                currentDayHigh = candle5m.high();
                currentDayLow = candle5m.low();

                // Извлекаем подтвержденные уровни ВЧЕРАШНЕГО дня для торговли СЕГОДНЯ
                BigDecimal[] yesterdayRange = historicalDailyRanges.get(candleDay.minusDays(1));
                if (yesterdayRange != null) {
                    yesterdayHigh = yesterdayRange[0];
                    yesterdayLow = yesterdayRange[1];
                } else {
                    yesterdayHigh = null;
                    yesterdayLow = null;
                }

                // Сброс триггеров торговли (новый день — чистый лист)
                isBreakoutConfirmed = false;
                breakoutSide = null;
                breakoutConfirmedAt = null;
                breakoutLevel = null;
                candles5m.clear();
                buffer15m.clear();
            } else {
                // Продолжаем обновлять экстремумы текущего дня
                currentDayHigh = currentDayHigh.max(candle5m.high());
                currentDayLow = currentDayLow.min(candle5m.low());
            }

            candles5m.add(candle5m);

            // Агрегируем в 15м
            buffer15m.add(candle5m);
            if (buffer15m.size() == 3) {
                Candle aggregated15m = aggregateCandles(buffer15m);
                buffer15m.clear();

                if (yesterdayHigh != null) {
                    // ЕСЛИ ПРОБОЯ ЕЩЕ НЕ БЫЛО: Ищем истинный пробой телом 15м свечи
                    if (!isBreakoutConfirmed) {
                        if (aggregated15m.close().compareTo(yesterdayHigh) > 0) {
                            isBreakoutConfirmed = true;
                            breakoutSide = Order.PositionSide.LONG;
                            breakoutLevel = yesterdayHigh;
                            breakoutConfirmedAt = aggregated15m.closeTime();
                        } else if (aggregated15m.close().compareTo(yesterdayLow) < 0) {
                            isBreakoutConfirmed = true;
                            breakoutSide = Order.PositionSide.SHORT;
                            breakoutLevel = yesterdayLow;
                            breakoutConfirmedAt = aggregated15m.closeTime();
                        }
                    }
                    // ЕСЛИ ПРОБОЙ УЖЕ БЫЛ АКТИВЕН: Проверяем, не вернулась ли цена внутрь диапазона (Ложный пробой)
                    else {
                        if (breakoutSide == Order.PositionSide.LONG && aggregated15m.close().compareTo(yesterdayHigh) <= 0) {
                            // Цена закрылась ниже или на уровне вчерашнего High — пробой аннулирован!
                            isBreakoutConfirmed = false;
                            breakoutSide = null;
                            breakoutConfirmedAt = null;
                            breakoutLevel = null;
                        } else if (breakoutSide == Order.PositionSide.SHORT && aggregated15m.close().compareTo(yesterdayLow) >= 0) {
                            // Цена закрылась выше или на уровне вчерашнего Low — пробой аннулирован!
                            isBreakoutConfirmed = false;
                            breakoutSide = null;
                            breakoutConfirmedAt = null;
                            breakoutLevel = null;
                        }
                    }
                }
            }
        }

        public Signal evaluateSignal(Candle current5m, String symbol, BigDecimal usdtAmount) {
            // Защита: Без вчерашних уровней или если рынок вернулся в рендж — не торгуем
            if (yesterdayHigh == null || !isBreakoutConfirmed) {
                return holdSignal(symbol);
            }

            // Свеча 5м должна закрыться строго ПОСЛЕ 15м подтверждения
            if (!current5m.openTime().isAfter(breakoutConfirmedAt)) {
                return holdSignal(symbol);
            }

            // Факт касания уровня
            boolean isRetest = current5m.low().compareTo(breakoutLevel) <= 0
                    && current5m.high().compareTo(breakoutLevel) >= 0;

            if (isRetest && candles5m.size() > 1) {
                Candle prev5m = candles5m.get(candles5m.size() - 2);

                if (breakoutSide == Order.PositionSide.LONG) {
                    if (isHammer(current5m)) {
                        return buildSignal(Signal.Action.BUY, symbol, usdtAmount, current5m.high(), "Breakout Retest LONG (Hammer)", current5m.openTime(), Order.PositionSide.LONG);
                    }
                    if (isBullishEngulfing(current5m, prev5m)) {
                        return buildSignal(Signal.Action.BUY, symbol, usdtAmount, prev5m.high(), "Breakout Retest LONG (Bullish Engulfing)", current5m.openTime(), Order.PositionSide.LONG);
                    }
                } else if (breakoutSide == Order.PositionSide.SHORT) {
                    if (isFallingStar(current5m)) {
                        return buildSignal(Signal.Action.SELL, symbol, usdtAmount, current5m.low(), "Breakout Retest SHORT (Falling Star)", current5m.openTime(), Order.PositionSide.SHORT);
                    }
                    if (isBearishEngulfing(current5m, prev5m)) {
                        return buildSignal(Signal.Action.SELL, symbol, usdtAmount, prev5m.low(), "Breakout Retest SHORT (Bearish Engulfing)", current5m.openTime(), Order.PositionSide.SHORT);
                    }
                }
            }

            return holdSignal(symbol);
        }

        private void updateDailyAggregations(Candle candle, LocalDate date) {
            historicalDailyRanges.compute(date, (k, currentRange) -> {
                if (currentRange == null) {
                    return new BigDecimal[]{candle.high(), candle.low()};
                }
                // Заменяем старые значения на максимальные/минимальные
                BigDecimal newHigh = currentRange[0].max(candle.high());
                BigDecimal newLow = currentRange[1].min(candle.low());
                return new BigDecimal[]{newHigh, newLow};
            });
        }

        private Candle aggregateCandles(List<Candle> list) {
            Candle first = list.get(0);
            Candle last = list.get(list.size() - 1);

            BigDecimal high = list.stream().map(Candle::high).reduce(BigDecimal::max).orElse(first.high());
            BigDecimal low = list.stream().map(Candle::low).reduce(BigDecimal::min).orElse(first.low());
            BigDecimal volume = list.stream().map(Candle::volume).reduce(BigDecimal.ZERO, BigDecimal::add);
            int ticks = list.stream().mapToInt(Candle::tickCount).sum();
            return Candle.builder().symbol(first.symbol()).openTime(first.openTime()).closeTime(last.closeTime()).open(first.open()).high(high).low(low).close(last.close()).volume(volume).tickCount(ticks).timeframe(java.time.Duration.ofMinutes(15)).build();
        }

        private boolean isHammer(Candle c) {
            if (c.wickRange().compareTo(BigDecimal.ZERO) == 0) return false;
            BigDecimal minOpenClose = c.open().min(c.close());
            BigDecimal maxOpenClose = c.open().max(c.close());
            BigDecimal lowerShadow = minOpenClose.subtract(c.low());
            BigDecimal upperShadow = c.high().subtract(maxOpenClose);
            return lowerShadow.compareTo(c.bodySize().multiply(TWO)) >= 0 && upperShadow.compareTo(c.wickRange().multiply(ZERO_POINT_TWO)) <= 0;
        }

        private boolean isFallingStar(Candle c) {
            if (c.wickRange().compareTo(BigDecimal.ZERO) == 0) return false;
            BigDecimal minOpenClose = c.open().min(c.close());
            BigDecimal maxOpenClose = c.open().max(c.close());
            BigDecimal lowerShadow = minOpenClose.subtract(c.low());
            BigDecimal upperShadow = c.high().subtract(maxOpenClose);
            return upperShadow.compareTo(c.bodySize().multiply(TWO)) >= 0 && lowerShadow.compareTo(c.wickRange().multiply(ZERO_POINT_TWO)) <= 0;
        }

        private boolean isBullishEngulfing(Candle curr, Candle prev) {
            return prev.isBearish() && curr.isBullish() && curr.low().compareTo(prev.low()) < 0 && curr.high().compareTo(prev.high()) > 0;
        }

        private boolean isBearishEngulfing(Candle curr, Candle prev) {
            return prev.isBullish() && curr.isBearish() && curr.low().compareTo(prev.low()) < 0 && curr.high().compareTo(prev.high()) > 0;
        }

        private Signal holdSignal(String symbol) {
            return Signal.builder().action(Signal.Action.HOLD).symbol(symbol).usdtAmount(BigDecimal.ZERO).limitPrice(BigDecimal.ZERO).timestamp(Instant.now()).build();
        }

        private Signal buildSignal(Signal.Action action, String symbol, BigDecimal usdtAmount, BigDecimal price, String reason, java.time.LocalDateTime time, Order.PositionSide side) {
            return Signal.builder().action(action).symbol(symbol).usdtAmount(usdtAmount).limitPrice(price).reason(reason).timestamp(time.toInstant(ZoneOffset.UTC)).positionSide(side).build();
        }
    }
}