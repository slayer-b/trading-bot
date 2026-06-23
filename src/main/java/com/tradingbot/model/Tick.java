package com.tradingbot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Tick(
        String    symbol,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal lastPrice,
        BigDecimal volume,
        Instant   timestamp
) {
    public BigDecimal midPrice() {
        return bid.add(ask).divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String symbol;
        private BigDecimal bid, ask, lastPrice, volume;
        private Instant timestamp;
        public Builder symbol(String v)        { this.symbol    = v; return this; }
        public Builder bid(BigDecimal v)       { this.bid       = v; return this; }
        public Builder ask(BigDecimal v)       { this.ask       = v; return this; }
        public Builder lastPrice(BigDecimal v) { this.lastPrice = v; return this; }
        public Builder volume(BigDecimal v)    { this.volume    = v; return this; }
        public Builder timestamp(Instant v)    { this.timestamp = v; return this; }
        public Tick build() { return new Tick(symbol, bid, ask, lastPrice, volume, timestamp); }
    }
}
