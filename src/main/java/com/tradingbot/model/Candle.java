package com.tradingbot.model;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;

public record Candle(
        String        symbol,
        LocalDateTime openTime,
        LocalDateTime closeTime,
        BigDecimal    open,
        BigDecimal    high,
        BigDecimal    low,
        BigDecimal    close,
        BigDecimal    volume,
        int           tickCount,
        Duration      timeframe
) {
    public boolean isBullish()  { return close.compareTo(open) > 0; }
    public boolean isBearish()  { return close.compareTo(open) < 0; }
    public BigDecimal bodySize(){ return close.subtract(open).abs(); }
    public BigDecimal wickRange(){ return high.subtract(low); }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String symbol; private LocalDateTime openTime, closeTime;
        private BigDecimal open, high, low, close;
        private BigDecimal volume = BigDecimal.ZERO;
        private int tickCount; private Duration timeframe;
        public Builder symbol(String v)          { this.symbol    = v; return this; }
        public Builder openTime(LocalDateTime v) { this.openTime  = v; return this; }
        public Builder closeTime(LocalDateTime v){ this.closeTime = v; return this; }
        public Builder open(BigDecimal v)        { this.open      = v; return this; }
        public Builder high(BigDecimal v)        { this.high      = v; return this; }
        public Builder low(BigDecimal v)         { this.low       = v; return this; }
        public Builder close(BigDecimal v)       { this.close     = v; return this; }
        public Builder volume(BigDecimal v)      { this.volume    = v; return this; }
        public Builder tickCount(int v)          { this.tickCount = v; return this; }
        public Builder timeframe(Duration v)     { this.timeframe = v; return this; }
        public Candle build() {
            return new Candle(symbol, openTime, closeTime, open, high, low,
                              close, volume, tickCount, timeframe);
        }
    }
}
