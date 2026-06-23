package com.tradingbot.core;

import com.tradingbot.model.Candle;
import com.tradingbot.model.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;

public class CandleAggregator {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregator.class);

    private final Duration    timeframe;
    private MutableCandle     current = null;

    public CandleAggregator(Duration timeframe) {
        if (timeframe.isZero() || timeframe.isNegative())
            throw new IllegalArgumentException("timeframe must be positive");
        this.timeframe = timeframe;
    }

    public long          timeframeMinutes()  { return timeframe.toMinutes(); }
    public LocalDateTime currentWindowEnd()  { return current != null ? current.windowEnd : null; }

    /**
     * Feed one tick. Returns a completed {@link Candle} when the window rolls,
     * {@code null} while the candle is still building.
     */
    public Candle onTick(Tick tick) {
        LocalDateTime now         = LocalDateTime.now();
        LocalDateTime windowStart = floorToWindow(now);
        LocalDateTime windowEnd   = windowStart.plus(timeframe);

        if (current == null) {
            current = new MutableCandle(tick.symbol(), windowStart, windowEnd, tick);
            log.debug("[Candle] First candle opened: {} → {}", windowStart, windowEnd);
            return null;
        }

        if (!now.isBefore(current.windowEnd)) {
            Candle closed = current.build();
            current = new MutableCandle(tick.symbol(), windowStart, windowEnd, tick);
            log.info("[Candle] {}m {} O={} H={} L={} C={} ticks={}",
                timeframe.toMinutes(), closed.symbol(),
                closed.open(), closed.high(), closed.low(), closed.close(), closed.tickCount());
            return closed;
        }

        current.update(tick);
        return null;
    }

    private LocalDateTime floorToWindow(LocalDateTime time) {
        long totalSeconds         = timeframe.toSeconds();
        long secondsSinceMidnight = time.toLocalTime().toSecondOfDay();
        long windowSeconds        = (secondsSinceMidnight / totalSeconds) * totalSeconds;
        return time.toLocalDate().atStartOfDay().plusSeconds(windowSeconds);
    }

    static final class MutableCandle {
        final String        symbol;
        final LocalDateTime windowStart;
        final LocalDateTime windowEnd;
        final Duration      timeframe;
        BigDecimal open, high, low, close, volume;
        int tickCount;

        MutableCandle(String symbol, LocalDateTime windowStart, LocalDateTime windowEnd, Tick first) {
            this.symbol      = symbol;
            this.windowStart = windowStart;
            this.windowEnd   = windowEnd;
            this.timeframe   = Duration.between(windowStart, windowEnd);
            BigDecimal price = first.midPrice();
            this.open = this.high = this.low = this.close = price;
            this.volume    = first.volume();
            this.tickCount = 1;
        }

        void update(Tick tick) {
            BigDecimal price = tick.midPrice();
            if (price.compareTo(high) > 0) high = price;
            if (price.compareTo(low)  < 0) low  = price;
            close  = price;
            volume = volume.add(tick.volume());
            tickCount++;
        }

        Candle build() {
            return Candle.builder()
                .symbol(symbol)
                .openTime(windowStart).closeTime(windowEnd)
                .open(open).high(high).low(low).close(close)
                .volume(volume).tickCount(tickCount).timeframe(timeframe)
                .build();
        }
    }
}
