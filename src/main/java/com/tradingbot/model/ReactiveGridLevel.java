package com.tradingbot.model;

import java.math.BigDecimal;

public class ReactiveGridLevel {
    private BigDecimal price;
    private boolean isBuyOrder;
    private boolean isActive;

    public ReactiveGridLevel(BigDecimal price, boolean isBuyOrder) {
        this.price = price;
        this.isBuyOrder = isBuyOrder;
        this.isActive = true;
    }

    public BigDecimal getPrice() { return price; }
    public boolean isBuyOrder() { return isBuyOrder; }
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { this.isActive = active; }

    public void updateLevel(BigDecimal newPrice, boolean isBuyOrder) {
        this.price = newPrice;
        this.isBuyOrder = isBuyOrder;
        this.isActive = true;
    }
}