package com.tradingbot.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

public record OrderBook(
        String                      symbol,
        NavigableMap<BigDecimal, BigDecimal> bids,   // descending — best bid first
        NavigableMap<BigDecimal, BigDecimal> asks,   // ascending  — best ask first
        long                        lastUpdateId,
        Instant                     timestamp
) {
    public OrderBook {
        // Defensive sorted copies
        TreeMap<BigDecimal, BigDecimal> b = new TreeMap<>(Collections.reverseOrder());
        b.putAll(bids);
        bids = Collections.unmodifiableNavigableMap(b);

        TreeMap<BigDecimal, BigDecimal> a = new TreeMap<>();
        a.putAll(asks);
        asks = Collections.unmodifiableNavigableMap(a);
    }

    // ---- Imbalance ----------------------------------------------------------

    public double weightedImbalance(int depth) {
        double bidW = 0, askW = 0;
        int i = 0;
        for (Map.Entry<BigDecimal, BigDecimal> e : bids.entrySet()) {
            if (i >= depth) break;
            bidW += e.getValue().doubleValue() / (i + 1);
            i++;
        }
        i = 0;
        for (Map.Entry<BigDecimal, BigDecimal> e : asks.entrySet()) {
            if (i >= depth) break;
            askW += e.getValue().doubleValue() / (i + 1);
            i++;
        }
        double total = bidW + askW;
        return total == 0 ? 0.5 : bidW / total;
    }

    public double simpleImbalance(int depth) {
        double bid = bids.values().stream().limit(depth).mapToDouble(BigDecimal::doubleValue).sum();
        double ask = asks.values().stream().limit(depth).mapToDouble(BigDecimal::doubleValue).sum();
        double t   = bid + ask;
        return t == 0 ? 0.5 : bid / t;
    }

    // ---- Best prices --------------------------------------------------------

    /** Best (highest) bid price, or ZERO if book is empty. */
    public BigDecimal bestBid() {
        return bids.isEmpty() ? BigDecimal.ZERO : bids.firstKey();
    }

    /** Best (lowest) ask price, or ZERO if book is empty. */
    public BigDecimal bestAsk() {
        return asks.isEmpty() ? BigDecimal.ZERO : asks.firstKey();
    }

    /** Absolute spread = bestAsk - bestBid. */
    public BigDecimal spread() {
        return bestAsk().subtract(bestBid());
    }

    /** Spread in basis points relative to mid price. */
    public double spreadBps() {
        BigDecimal mid = bestBid().add(bestAsk())
            .divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
        if (mid.compareTo(BigDecimal.ZERO) == 0) return 0;
        return spread()
            .divide(mid, 8, RoundingMode.HALF_UP)
            .doubleValue() * 10_000.0;
    }

    @Override
    public String toString() {
        return "OrderBook{%s bid=%s ask=%s imb=%.3f spread=%.2fbps}".formatted(
            symbol, bestBid(), bestAsk(),
            weightedImbalance(10), spreadBps());
    }
}
