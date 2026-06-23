package com.tradingbot.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;

public record BalanceSnapshot(
        Map<String, BigDecimal> balances,
        BigDecimal              totalValueUsdt,
        LocalDateTime           snapshotTime
) {
    public BalanceSnapshot {
        balances = Collections.unmodifiableMap(Map.copyOf(balances));
    }
    public BigDecimal balance(String asset) {
        return balances.getOrDefault(asset, BigDecimal.ZERO);
    }
}
