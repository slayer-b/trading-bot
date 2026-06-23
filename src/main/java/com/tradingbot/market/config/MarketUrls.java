package com.tradingbot.market.config;

public record MarketUrls(
        String wsBase,
        String restBase,
        String tickPath,
        String accountPath,
        String orderPath
) {
    public String wsUri(String symbol) {
        return wsBase + "/" + tickPath.replace("{symbol}", symbol.toLowerCase());
    }
}
