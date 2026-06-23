package com.tradingbot.model;

import java.math.BigDecimal;
import java.time.Instant;

public record Signal(
        Action             action,
        String             symbol,
        BigDecimal         usdtAmount,
        BigDecimal         limitPrice,
        String             reason,
        Instant            timestamp,
        Integer            leverage,
        Order.PositionSide positionSide
) {
    public enum Action { BUY, SELL, HOLD }
    public boolean isFutures() { return leverage != null; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private Action action; private String symbol;
        private BigDecimal usdtAmount, limitPrice;
        private String reason; private Instant timestamp;
        private Integer leverage; private Order.PositionSide positionSide;
        public Builder action(Action v)                   { this.action = v; return this; }
        public Builder symbol(String v)                   { this.symbol = v; return this; }
        public Builder usdtAmount(BigDecimal v)           { this.usdtAmount = v; return this; }
        public Builder limitPrice(BigDecimal v)           { this.limitPrice = v; return this; }
        public Builder reason(String v)                   { this.reason = v; return this; }
        public Builder timestamp(Instant v)               { this.timestamp = v; return this; }
        public Builder leverage(Integer v)                { this.leverage = v; return this; }
        public Builder positionSide(Order.PositionSide v) { this.positionSide = v; return this; }
        public Signal build() {
            return new Signal(action, symbol, usdtAmount, limitPrice,
                              reason, timestamp, leverage, positionSide);
        }
    }
}
