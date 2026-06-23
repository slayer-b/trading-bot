package com.tradingbot.model;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

public record AccountState(
        Map<String, BigDecimal> balances,
        Map<String, Order>      openOrders,
        Instant                 snapshotTime,
        BalanceHistory          balanceHistory,
        BigDecimal              unrealisedPnlUsdt
) {
    public AccountState {
        balances          = Collections.unmodifiableMap(balances);
        openOrders        = Collections.unmodifiableMap(openOrders);
        unrealisedPnlUsdt = unrealisedPnlUsdt != null ? unrealisedPnlUsdt : BigDecimal.ZERO;
    }

    public BigDecimal balance(String asset) {
        return balances.getOrDefault(asset, BigDecimal.ZERO);
    }

    public BigDecimal totalValueUsdt(Map<String, BigDecimal> prices, String quoteAsset) {
        BigDecimal total = balance(quoteAsset).add(unrealisedPnlUsdt);
        for (Map.Entry<String, BigDecimal> e : balances.entrySet()) {
            String asset = e.getKey();
            if (asset.equals(quoteAsset)) continue;
            BigDecimal price = prices.get(asset + quoteAsset);
            if (price == null) price = prices.get(quoteAsset + asset);
            if (price != null)
                total = total.add(e.getValue().multiply(price, MathContext.DECIMAL64));
        }
        return total;
    }

    public AccountState withBalances(Map<String, BigDecimal> v) {
        return new AccountState(v, openOrders, snapshotTime, balanceHistory, unrealisedPnlUsdt);
    }
    public AccountState withOpenOrders(Map<String, Order> v) {
        return new AccountState(balances, v, snapshotTime, balanceHistory, unrealisedPnlUsdt);
    }
    public AccountState withSnapshotTime(Instant v) {
        return new AccountState(balances, openOrders, v, balanceHistory, unrealisedPnlUsdt);
    }
    public AccountState withBalanceHistory(BalanceHistory v) {
        return new AccountState(balances, openOrders, snapshotTime, v, unrealisedPnlUsdt);
    }
    public AccountState withUnrealisedPnl(BigDecimal v) {
        return new AccountState(balances, openOrders, snapshotTime, balanceHistory, v);
    }
    public AccountState recordSnapshot(Map<String, BigDecimal> prices, String quoteAsset) {
        BigDecimal total = totalValueUsdt(prices, quoteAsset);
        return withBalanceHistory(
            balanceHistory.append(new BalanceSnapshot(balances, total, LocalDateTime.now())));
    }

    public Builder toBuilder() { return new Builder(this); }
    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private Map<String, BigDecimal> balances        = Collections.emptyMap();
        private Map<String, Order>      openOrders      = Collections.emptyMap();
        private Instant                 snapshotTime;
        private BalanceHistory          balanceHistory   = BalanceHistory.withCapacity(100);
        private BigDecimal              unrealisedPnlUsdt = BigDecimal.ZERO;

        Builder() {}
        Builder(AccountState s) {
            balances = s.balances(); openOrders = s.openOrders();
            snapshotTime = s.snapshotTime(); balanceHistory = s.balanceHistory();
            unrealisedPnlUsdt = s.unrealisedPnlUsdt();
        }
        public Builder balances(Map<String, BigDecimal> v) { this.balances        = v; return this; }
        public Builder openOrders(Map<String, Order> v)    { this.openOrders      = v; return this; }
        public Builder snapshotTime(Instant v)             { this.snapshotTime    = v; return this; }
        public Builder balanceHistory(BalanceHistory v)    { this.balanceHistory  = v; return this; }
        public Builder unrealisedPnlUsdt(BigDecimal v)     { this.unrealisedPnlUsdt = v; return this; }
        public AccountState build() {
            return new AccountState(balances, openOrders, snapshotTime,
                                    balanceHistory, unrealisedPnlUsdt);
        }
    }
}
