package com.tradingbot.bot;

import java.math.BigDecimal;
import java.math.RoundingMode;

public record BotResult(
        String     name,
        String     symbol,
        String     strategy,
        String     market,
        int        candleTimeframeMinutes,
        BigDecimal initialUsdt,
        BigDecimal finalUsdt,
        int        totalTrades,
        int        wins,
        int        losses,
        BigDecimal maxDrawdownPct,
        BigDecimal commissionPaid
) {
    public BigDecimal returnPct() {
        if (initialUsdt.compareTo(BigDecimal.ZERO) == 0) return BigDecimal.ZERO;
        return finalUsdt.subtract(initialUsdt)
            .divide(initialUsdt, 6, RoundingMode.HALF_UP)
            .multiply(BigDecimal.valueOf(100))
            .setScale(2, RoundingMode.HALF_UP);
    }

    public double winRate() {
        int closed = wins + losses;
        return closed == 0 ? 0.0 : (double) wins / closed * 100.0;
    }

    public BigDecimal profitUsdt() {
        return finalUsdt.subtract(initialUsdt).setScale(2, RoundingMode.HALF_UP);
    }
}
