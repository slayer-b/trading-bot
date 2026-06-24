package com.tradingbot.strategy.helper;

import com.tradingbot.model.Candle;
import com.tradingbot.model.Order;
import com.tradingbot.model.Signal;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Isolated market state tracking logic for Breakout & Retest strategy.
 * Handles Daily boundary calculation, 15m breakout confirmation, and 5m entry patterns with 1:3 R:R levels.
 */
public class MarketStateTracker {
    private static final BigDecimal TWO = new BigDecimal("2");
    private static final BigDecimal ZERO_POINT_TWO = new BigDecimal("0.2");
    private static final BigDecimal THREE = new BigDecimal("3");

    private final Map<LocalDate, BigDecimal[]> historicalDailyRanges = new HashMap<>();
    private final List<Candle> candles5m = new ArrayList<>();
    private final List<Candle> buffer15m = new ArrayList<>();

    private LocalDate currentTrackingDay = null;
    private BigDecimal currentDayHigh = null;
    private BigDecimal currentDayLow = null;

    private BigDecimal yesterdayHigh = null;
    private BigDecimal yesterdayLow = null;
    private boolean isBreakoutConfirmed = false;
    private Order.PositionSide breakoutSide = null;
    private LocalDateTime breakoutConfirmedAt = null;
    private BigDecimal breakoutLevel = null;

    public void updateState(Candle candle5m) {
        LocalDate candleDay = candle5m.openTime().toLocalDate();

        if (currentTrackingDay == null) {
            currentTrackingDay = candleDay;
            currentDayHigh = candle5m.high();
            currentDayLow = candle5m.low();
        } else if (!candleDay.equals(currentTrackingDay)) {
            // Save completed daily high and low range into history
            historicalDailyRanges.put(currentTrackingDay, new BigDecimal[]{currentDayHigh, currentDayLow});

            currentTrackingDay = candleDay;
            currentDayHigh = candle5m.high();
            currentDayLow = candle5m.low();

            // Load yesterday's trading boundaries for the current live session
            BigDecimal[] range = historicalDailyRanges.get(candleDay.minusDays(1));
            yesterdayHigh = range != null ? range[0] : null;
            yesterdayLow = range != null ? range[1] : null;

            resetBreakout();
            candles5m.clear();
            buffer15m.clear();
        } else {
            currentDayHigh = currentDayHigh.max(candle5m.high());
            currentDayLow = currentDayLow.min(candle5m.low());
        }

        candles5m.add(candle5m);
        buffer15m.add(candle5m);

        // Aggregate three 5m candles into one distinct 15m candle structure
        if (buffer15m.size() == 3) {
            Candle agg15m = aggregate15m(buffer15m);
            buffer15m.clear();

            if (yesterdayHigh != null) {
                if (!isBreakoutConfirmed) {
                    // Confirm breakout strictly via the 15m candle close price boundary cross
                    if (agg15m.close().compareTo(yesterdayHigh) > 0) {
                        setBreakout(Order.PositionSide.LONG, yesterdayHigh, agg15m.closeTime());
                    } else if (agg15m.close().compareTo(yesterdayLow) < 0) {
                        setBreakout(Order.PositionSide.SHORT, yesterdayLow, agg15m.closeTime());
                    }
                } else {
                    // Fakeout Protection: Reset breakout if 15m candle body slips back inside yesterday's range
                    if (breakoutSide == Order.PositionSide.LONG && agg15m.close().compareTo(yesterdayHigh) <= 0) resetBreakout();
                    else if (breakoutSide == Order.PositionSide.SHORT && agg15m.close().compareTo(yesterdayLow) >= 0) resetBreakout();
                }
            }
        }
    }

    public Signal evaluateSignal(Candle c5m, String symbol, BigDecimal usdtAmount) {
        if (yesterdayHigh == null || !isBreakoutConfirmed || !c5m.openTime().isAfter(breakoutConfirmedAt)) {
            return holdSignal(symbol);
        }

        // Mathematical verification of a level retest event
        boolean isRetest = c5m.low().compareTo(breakoutLevel) <= 0 && c5m.high().compareTo(breakoutLevel) >= 0;

        if (isRetest && candles5m.size() > 1) {
            Candle prev5m = candles5m.get(candles5m.size() - 2);
            if (breakoutSide == Order.PositionSide.LONG) {
                if (isHammer(c5m)) {
                    BigDecimal entry = c5m.high();
                    BigDecimal sl = c5m.low();
                    BigDecimal tp = entry.add(entry.subtract(sl).multiply(THREE));
                    return buildSignal(Signal.Action.BUY, symbol, usdtAmount, entry, "LONG (Hammer)", entry, sl, tp, c5m.openTime(), Order.PositionSide.LONG);
                }
                if (isBullishEngulfing(c5m, prev5m)) {
                    BigDecimal entry = prev5m.high();
                    BigDecimal sl = c5m.low();
                    BigDecimal tp = entry.add(entry.subtract(sl).multiply(THREE));
                    return buildSignal(Signal.Action.BUY, symbol, usdtAmount, entry, "LONG (Engulfing)", entry, sl, tp, c5m.openTime(), Order.PositionSide.LONG);
                }
            } else {
                if (isFallingStar(c5m)) {
                    BigDecimal entry = c5m.low();
                    BigDecimal sl = c5m.high();
                    BigDecimal tp = entry.subtract(sl.subtract(entry).multiply(THREE));
                    return buildSignal(Signal.Action.SELL, symbol, usdtAmount, entry, "SHORT (Falling Star)", entry, sl, tp, c5m.openTime(), Order.PositionSide.SHORT);
                }
                if (isBearishEngulfing(c5m, prev5m)) {
                    BigDecimal entry = prev5m.low();
                    BigDecimal sl = c5m.high();
                    BigDecimal tp = entry.subtract(sl.subtract(entry).multiply(THREE));
                    return buildSignal(Signal.Action.SELL, symbol, usdtAmount, entry, "SHORT (Engulfing)", entry, sl, tp, c5m.openTime(), Order.PositionSide.SHORT);
                }
            }
        }
        return holdSignal(symbol);
    }

    private void setBreakout(Order.PositionSide side, BigDecimal lvl, LocalDateTime time) {
        isBreakoutConfirmed = true; breakoutSide = side; breakoutLevel = lvl; breakoutConfirmedAt = time;
    }
    private void resetBreakout() {
        isBreakoutConfirmed = false; breakoutSide = null; breakoutLevel = null; breakoutConfirmedAt = null;
    }

    private Candle aggregate15m(List<Candle> list) {
        Candle first = list.get(0); Candle last = list.get(2);
        BigDecimal h = list.stream().map(Candle::high).reduce(BigDecimal::max).orElse(first.high());
        BigDecimal l = list.stream().map(Candle::low).reduce(BigDecimal::min).orElse(first.low());
        return Candle.builder()
                .symbol(first.symbol()).openTime(first.openTime()).closeTime(last.closeTime())
                .open(first.open()).high(h).low(l).close(last.close()).volume(BigDecimal.ZERO)
                .timeframe(java.time.Duration.ofMinutes(15)).build();
    }

    private boolean isHammer(Candle c) {
        if (c.wickRange().compareTo(BigDecimal.ZERO) == 0) return false;
        BigDecimal minOpenClose = c.open().min(c.close());
        BigDecimal maxOpenClose = c.open().max(c.close());
        return minOpenClose.subtract(c.low()).compareTo(c.bodySize().multiply(TWO)) >= 0
                && c.high().subtract(maxOpenClose).compareTo(c.wickRange().multiply(ZERO_POINT_TWO)) <= 0;
    }

    private boolean isFallingStar(Candle c) {
        if (c.wickRange().compareTo(BigDecimal.ZERO) == 0) return false;
        BigDecimal minOpenClose = c.open().min(c.close());
        BigDecimal maxOpenClose = c.open().max(c.close());
        return maxOpenClose.subtract(minOpenClose).compareTo(c.bodySize().multiply(TWO)) >= 0
                && minOpenClose.subtract(c.low()).compareTo(c.wickRange().multiply(ZERO_POINT_TWO)) <= 0;
    }

    private boolean isBullishEngulfing(Candle curr, Candle prev) { return prev.isBearish() && curr.isBullish() && curr.low().compareTo(prev.low()) < 0 && curr.high().compareTo(prev.high()) > 0; }
    private boolean isBearishEngulfing(Candle curr, Candle prev) { return prev.isBullish() && curr.isBearish() && curr.low().compareTo(prev.low()) < 0 && curr.high().compareTo(prev.high()) > 0; }

    private Signal holdSignal(String sym) {
        return Signal.builder().action(Signal.Action.HOLD).symbol(sym).usdtAmount(BigDecimal.ZERO).limitPrice(BigDecimal.ZERO).timestamp(Instant.now()).build();
    }

    private Signal buildSignal(Signal.Action act, String sym, BigDecimal amt, BigDecimal prc, String patternName, BigDecimal entry, BigDecimal sl, BigDecimal tp, LocalDateTime t, Order.PositionSide s) {
        // Standardize brackets description to safely pass SL/TP inside the string reason field
        String structuralReason = String.format("%s | Entry: %.4f | SL: %.4f | TP: %.4f", patternName, entry, sl, tp);
        return Signal.builder()
                .action(act)
                .symbol(sym)
                .usdtAmount(amt)
                .limitPrice(prc)
                .reason(structuralReason)
                .timestamp(t.toInstant(ZoneOffset.UTC))
                .positionSide(s)
                .build();
    }
}
