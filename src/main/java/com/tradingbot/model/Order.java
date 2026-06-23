package com.tradingbot.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Order(
        String       clientOrderId,
        String       exchangeOrderId,
        String       symbol,
        Side         side,
        Type         type,
        BigDecimal   quantity,
        BigDecimal   limitPrice,
        BigDecimal   fillPrice,
        BigDecimal   filledQuantity,
        Status       status,
        Instant      createdAt,
        Instant      updatedAt,
        Integer      leverage,
        PositionSide positionSide,
        Boolean      reduceOnly
) {
    public enum Side         { BUY, SELL }
    public enum Type         { MARKET, LIMIT }
    public enum Status       { PENDING, FILLED, PARTIALLY_FILLED, CANCELLED, REJECTED }
    public enum PositionSide { LONG, SHORT, BOTH }

    public Order {
        if (clientOrderId == null) clientOrderId = UUID.randomUUID().toString();
    }

    public boolean isFutures() { return leverage != null; }

    public Builder toBuilder() { return new Builder(this); }
    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String clientOrderId, exchangeOrderId, symbol;
        private Side side; private Type type;
        private BigDecimal quantity, limitPrice, fillPrice, filledQuantity;
        private Status status; private Instant createdAt, updatedAt;
        private Integer leverage; private PositionSide positionSide; private Boolean reduceOnly;

        Builder() {}
        Builder(Order o) {
            this.clientOrderId = o.clientOrderId(); this.exchangeOrderId = o.exchangeOrderId();
            this.symbol = o.symbol(); this.side = o.side(); this.type = o.type();
            this.quantity = o.quantity(); this.limitPrice = o.limitPrice();
            this.fillPrice = o.fillPrice(); this.filledQuantity = o.filledQuantity();
            this.status = o.status(); this.createdAt = o.createdAt(); this.updatedAt = o.updatedAt();
            this.leverage = o.leverage(); this.positionSide = o.positionSide();
            this.reduceOnly = o.reduceOnly();
        }
        public Builder clientOrderId(String v)      { this.clientOrderId   = v; return this; }
        public Builder exchangeOrderId(String v)    { this.exchangeOrderId = v; return this; }
        public Builder symbol(String v)             { this.symbol          = v; return this; }
        public Builder side(Side v)                 { this.side            = v; return this; }
        public Builder type(Type v)                 { this.type            = v; return this; }
        public Builder quantity(BigDecimal v)       { this.quantity        = v; return this; }
        public Builder limitPrice(BigDecimal v)     { this.limitPrice      = v; return this; }
        public Builder fillPrice(BigDecimal v)      { this.fillPrice       = v; return this; }
        public Builder filledQuantity(BigDecimal v) { this.filledQuantity  = v; return this; }
        public Builder status(Status v)             { this.status          = v; return this; }
        public Builder createdAt(Instant v)         { this.createdAt       = v; return this; }
        public Builder updatedAt(Instant v)         { this.updatedAt       = v; return this; }
        public Builder leverage(Integer v)          { this.leverage        = v; return this; }
        public Builder positionSide(PositionSide v) { this.positionSide    = v; return this; }
        public Builder reduceOnly(Boolean v)        { this.reduceOnly      = v; return this; }
        public Order build() {
            return new Order(clientOrderId, exchangeOrderId, symbol, side, type,
                             quantity, limitPrice, fillPrice, filledQuantity,
                             status, createdAt, updatedAt, leverage, positionSide, reduceOnly);
        }
    }
}
