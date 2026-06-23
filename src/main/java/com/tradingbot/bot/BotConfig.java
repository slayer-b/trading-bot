package com.tradingbot.bot;

import java.math.BigDecimal;

/**
 * All configuration for a single bot instance, fully resolved from
 * common.yml merged with the bot's own current/*.yml.
 */
public record BotConfig(
        String     name,
        // market
        String     market,
        String     symbol,
        String     quoteAsset,
        BigDecimal initialUsdt,
        BigDecimal usdtAmount,
        int        candleTimeframeMinutes,
        // strategy
        String     strategyName,
        // volume filter (0 = disabled)
        int        volumeFilterLookback,
        double     volumeFilterThreshold,
        // order book imbalance strategy params
        int        obDepth,
        double     obBuyThreshold,
        double     obSellThreshold,
        int        obConfirmationTicks,
        double     obMinSpreadBps,
        // lstm strategy params
        int        lstmLookback,
        int        lstmMinTrainCandles,
        int        lstmRetrainEvery,
        double     lstmBuyThreshold,
        double     lstmSellThreshold,
        int        emaFast,
        int        emaSlow,
        int        rsiPeriod,
        double     rsiOversold,
        double     rsiOverbought,
        int        macdFast,
        int        macdSlow,
        int        macdSignal,
        int        emaRsiFast,
        int        emaRsiSlow,
        int        emaRsiRsiPeriod,
        double     emaRsiOversold,
        double     emaRsiOverbought,
        // account
        int        historyCapacity,
        // binance
        String     binanceApiKey,
        String     binanceSecretKey,
        BigDecimal binanceCommission,
        // binance-alpha
        String     binanceAlphaApiKey,
        String     binanceAlphaSecretKey,
        BigDecimal binanceAlphaCommission,
        // okx
        String     okxApiKey,
        String     okxSecretKey,
        String     okxPassphrase,
        BigDecimal okxCommission,
        // binance futures
        String     binanceFutApiKey,
        String     binanceFutSecretKey,
        BigDecimal binanceFutCommission,
        boolean    binanceFutHedgeMode,
        // okx futures
        String     okxFutApiKey,
        String     okxFutSecretKey,
        String     okxFutPassphrase,
        BigDecimal okxFutCommission,
        boolean    okxFutHedgeMode,
        // urls (from common)
        String     binanceWsBase,
        String     binanceRestBase,
        String     binanceTickPath,
        String     binanceAccountPath,
        String     binanceOrderPath,
        String     binanceAlphaWsBase,
        String     binanceAlphaRestBase,
        String     binanceAlphaTickPath,
        String     binanceAlphaAccountPath,
        String     binanceAlphaOrderPath,
        String     okxWsBase,
        String     okxRestBase,
        String     okxTickPath,
        String     okxAccountPath,
        String     okxOrderPath,
        String     binanceFutWsBase,
        String     binanceFutRestBase,
        String     binanceFutTickPath,
        String     binanceFutAccountPath,
        String     binanceFutOrderPath,
        String     okxFutWsBase,
        String     okxFutRestBase,
        String     okxFutTickPath,
        String     okxFutAccountPath,
        String     okxFutOrderPath
) {}
