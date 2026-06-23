package com.tradingbot.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

public record BalanceHistory(List<BalanceSnapshot> snapshots, int capacity) {

    public BalanceHistory {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        snapshots = Collections.unmodifiableList(
            snapshots.stream()
                .sorted(Comparator.comparing(BalanceSnapshot::snapshotTime))
                .toList());
    }

    public static BalanceHistory withCapacity(int capacity) {
        return new BalanceHistory(List.of(), capacity);
    }

    public BalanceHistory append(BalanceSnapshot snapshot) {
        Deque<BalanceSnapshot> next = new ArrayDeque<>(snapshots);
        next.addLast(snapshot);
        while (next.size() > capacity) next.removeFirst();
        return new BalanceHistory(List.copyOf(next), capacity);
    }

    public List<MoneyPoint> moneyHistory(String asset) {
        return snapshots.stream()
            .map(s -> new MoneyPoint(s.snapshotTime(), s.balance(asset), s.totalValueUsdt()))
            .toList();
    }

    public record MoneyPoint(LocalDateTime time, BigDecimal amount, BigDecimal totalValueUsdt) {}

    public BalanceSnapshot latest() {
        return snapshots.isEmpty() ? null : snapshots.get(snapshots.size() - 1);
    }
    public BalanceSnapshot oldest() {
        return snapshots.isEmpty() ? null : snapshots.get(0);
    }
    public int    size()    { return snapshots.size(); }
    public boolean isEmpty() { return snapshots.isEmpty(); }
}
