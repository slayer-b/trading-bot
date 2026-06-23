package com.tradingbot.bot;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * All values from {@code common.yml} exposed as a Spring bean.
 * All YAML keys use camelCase — no hyphens — so @Value binding works correctly.
 */
@Component
public class CommonConfig {

    @Value("${trading.simulation.durationMinutes:120}") private int        simulationDurationMinutes;
    @Value("${trading.simulation.initialUsdt:10000}")   private BigDecimal initialUsdt;
    @Value("${trading.simulation.usdtAmount:100}")      private BigDecimal usdtAmount;

    @Value("${trading.market:binance-notrade}")         private String     market;
    @Value("${trading.symbol:BTCUSDT}")                 private String     symbol;
    @Value("${trading.strategyName:ema}")               private String     strategyName;
    @Value("${trading.candleTimeframeMinutes:5}")        private int        candleTimeframeMinutes;
    @Value("${trading.account.historyCapacity:200}")    private int        historyCapacity;
    @Value("${trading.account.quoteAsset:USDT}")        private String     quoteAsset;

    // volume filter
    @Value("${trading.volumeFilter.lookback:0}")        private int    volumeFilterLookback;
    @Value("${trading.volumeFilter.threshold:1.2}")     private double volumeFilterThreshold;

    // strategy params — ema
    @Value("${trading.strategyParams.ema.fastPeriod:9}")  private int emaFast;
    @Value("${trading.strategyParams.ema.slowPeriod:21}") private int emaSlow;

    // strategy params — rsi
    @Value("${trading.strategyParams.rsi.period:14}")         private int    rsiPeriod;
    @Value("${trading.strategyParams.rsi.oversold:30.0}")     private double rsiOversold;
    @Value("${trading.strategyParams.rsi.overbought:70.0}")   private double rsiOverbought;

    // strategy params — macd
    @Value("${trading.strategyParams.macd.fastPeriod:12}")    private int macdFast;
    @Value("${trading.strategyParams.macd.slowPeriod:26}")    private int macdSlow;
    @Value("${trading.strategyParams.macd.signalPeriod:9}")   private int macdSignal;

    // strategy params — emaRsi (was ema-rsi)
    @Value("${trading.strategyParams.emaRsi.emaFastPeriod:9}")   private int    emaRsiFast;
    @Value("${trading.strategyParams.emaRsi.emaSlowPeriod:21}")  private int    emaRsiSlow;
    @Value("${trading.strategyParams.emaRsi.rsiPeriod:14}")      private int    emaRsiRsiPeriod;
    @Value("${trading.strategyParams.emaRsi.oversold:35.0}")     private double emaRsiOversold;
    @Value("${trading.strategyParams.emaRsi.overbought:65.0}")   private double emaRsiOverbought;

    // strategy params — order book imbalance
    @Value("${trading.strategyParams.orderBookImbalance.depth:10}")              private int    obDepth;
    @Value("${trading.strategyParams.orderBookImbalance.buyThreshold:0.65}")     private double obBuyThreshold;
    @Value("${trading.strategyParams.orderBookImbalance.sellThreshold:0.35}")    private double obSellThreshold;
    @Value("${trading.strategyParams.orderBookImbalance.confirmationTicks:3}")   private int    obConfirmationTicks;
    @Value("${trading.strategyParams.orderBookImbalance.minSpreadBps:5.0}")      private double obMinSpreadBps;

    // credentials
    @Value("${trading.binance.apiKey:}")              private String     binanceApiKey;
    @Value("${trading.binance.secretKey:}")           private String     binanceSecretKey;
    @Value("${trading.binance.commissionRate:0.001}") private BigDecimal binanceCommission;

    @Value("${trading.binanceAlpha.apiKey:}")              private String     binanceAlphaApiKey;
    @Value("${trading.binanceAlpha.secretKey:}")           private String     binanceAlphaSecretKey;
    @Value("${trading.binanceAlpha.commissionRate:0.001}") private BigDecimal binanceAlphaCommission;

    @Value("${trading.okx.apiKey:}")              private String     okxApiKey;
    @Value("${trading.okx.secretKey:}")           private String     okxSecretKey;
    @Value("${trading.okx.passphrase:}")          private String     okxPassphrase;
    @Value("${trading.okx.commissionRate:0.001}") private BigDecimal okxCommission;

    @Value("${trading.binanceFutures.apiKey:}")              private String     binanceFutApiKey;
    @Value("${trading.binanceFutures.secretKey:}")           private String     binanceFutSecretKey;
    @Value("${trading.binanceFutures.commissionRate:0.0004}")private BigDecimal binanceFutCommission;
    @Value("${trading.binanceFutures.hedgeMode:false}")      private boolean    binanceFutHedgeMode;

    @Value("${trading.okxFutures.apiKey:}")              private String     okxFutApiKey;
    @Value("${trading.okxFutures.secretKey:}")           private String     okxFutSecretKey;
    @Value("${trading.okxFutures.passphrase:}")          private String     okxFutPassphrase;
    @Value("${trading.okxFutures.commissionRate:0.0005}")private BigDecimal okxFutCommission;
    @Value("${trading.okxFutures.hedgeMode:false}")      private boolean    okxFutHedgeMode;

    // binance urls
    @Value("${trading.urls.binance.wsBase}")      private String binanceWsBase;
    @Value("${trading.urls.binance.restBase}")    private String binanceRestBase;
    @Value("${trading.urls.binance.tickPath}")    private String binanceTickPath;
    @Value("${trading.urls.binance.accountPath}") private String binanceAccountPath;
    @Value("${trading.urls.binance.orderPath}")   private String binanceOrderPath;

    // binanceAlpha urls
    @Value("${trading.urls.binanceAlpha.wsBase}")      private String binanceAlphaWsBase;
    @Value("${trading.urls.binanceAlpha.restBase}")    private String binanceAlphaRestBase;
    @Value("${trading.urls.binanceAlpha.tickPath}")    private String binanceAlphaTickPath;
    @Value("${trading.urls.binanceAlpha.accountPath}") private String binanceAlphaAccountPath;
    @Value("${trading.urls.binanceAlpha.orderPath}")   private String binanceAlphaOrderPath;

    // okx urls
    @Value("${trading.urls.okx.wsBase}")      private String okxWsBase;
    @Value("${trading.urls.okx.restBase}")    private String okxRestBase;
    @Value("${trading.urls.okx.tickPath}")    private String okxTickPath;
    @Value("${trading.urls.okx.accountPath}") private String okxAccountPath;
    @Value("${trading.urls.okx.orderPath}")   private String okxOrderPath;

    // binanceFutures urls
    @Value("${trading.urls.binanceFutures.wsBase:wss://fstream.binance.com/ws}")
    private String binanceFutWsBase;
    @Value("${trading.urls.binanceFutures.restBase:https://fapi.binance.com}")
    private String binanceFutRestBase;
    @Value("${trading.urls.binanceFutures.tickPath:{symbol}@bookTicker}")
    private String binanceFutTickPath;
    @Value("${trading.urls.binanceFutures.accountPath:/fapi/v2/account}")
    private String binanceFutAccountPath;
    @Value("${trading.urls.binanceFutures.orderPath:/fapi/v1/order}")
    private String binanceFutOrderPath;

    // okxFutures urls
    @Value("${trading.urls.okxFutures.wsBase:wss://ws.okx.com:8443/ws/v5/public}")
    private String okxFutWsBase;
    @Value("${trading.urls.okxFutures.restBase:https://www.okx.com}")
    private String okxFutRestBase;
    @Value("${trading.urls.okxFutures.tickPath:}")
    private String okxFutTickPath;
    @Value("${trading.urls.okxFutures.accountPath:/api/v5/account/balance}")
    private String okxFutAccountPath;
    @Value("${trading.urls.okxFutures.orderPath:/api/v5/trade/order}")
    private String okxFutOrderPath;

    // ---- accessors ----------------------------------------------------------

    public int        simulationDurationMinutes() { return simulationDurationMinutes; }
    public BigDecimal initialUsdt()               { return initialUsdt; }
    public BigDecimal usdtAmount()                { return usdtAmount; }
    public String     market()                    { return market; }
    public String     symbol()                    { return symbol; }
    public String     strategyName()              { return strategyName; }
    public int        candleTimeframeMinutes()    { return candleTimeframeMinutes; }
    public int        historyCapacity()           { return historyCapacity; }
    public String     quoteAsset()                { return quoteAsset; }
    public int        volumeFilterLookback()      { return volumeFilterLookback; }
    public double     volumeFilterThreshold()     { return volumeFilterThreshold; }
    public int        emaFast()                   { return emaFast; }
    public int        emaSlow()                   { return emaSlow; }
    public int        rsiPeriod()                 { return rsiPeriod; }
    public double     rsiOversold()               { return rsiOversold; }
    public double     rsiOverbought()             { return rsiOverbought; }
    public int        macdFast()                  { return macdFast; }
    public int        macdSlow()                  { return macdSlow; }
    public int        macdSignal()                { return macdSignal; }
    public int        emaRsiFast()                { return emaRsiFast; }
    public int        emaRsiSlow()                { return emaRsiSlow; }
    public int        emaRsiRsiPeriod()           { return emaRsiRsiPeriod; }
    public double     emaRsiOversold()            { return emaRsiOversold; }
    public double     emaRsiOverbought()          { return emaRsiOverbought; }
    public int        obDepth()                   { return obDepth; }
    public double     obBuyThreshold()            { return obBuyThreshold; }
    public double     obSellThreshold()           { return obSellThreshold; }
    public int        obConfirmationTicks()       { return obConfirmationTicks; }
    public double     obMinSpreadBps()            { return obMinSpreadBps; }

    @Value("${trading.strategyParams.lstm.lookback:30}")          private int    lstmLookback;
    @Value("${trading.strategyParams.lstm.minTrainCandles:60}")   private int    lstmMinTrainCandles;
    @Value("${trading.strategyParams.lstm.retrainEvery:20}")      private int    lstmRetrainEvery;
    @Value("${trading.strategyParams.lstm.buyThreshold:0.60}")    private double lstmBuyThreshold;
    @Value("${trading.strategyParams.lstm.sellThreshold:0.40}")   private double lstmSellThreshold;

    public int    lstmLookback()         { return lstmLookback; }
    public int    lstmMinTrainCandles()  { return lstmMinTrainCandles; }
    public int    lstmRetrainEvery()     { return lstmRetrainEvery; }
    public double lstmBuyThreshold()     { return lstmBuyThreshold; }
    public double lstmSellThreshold()    { return lstmSellThreshold; }

    public String     binanceApiKey()             { return binanceApiKey; }
    public String     binanceSecretKey()          { return binanceSecretKey; }
    public BigDecimal binanceCommission()         { return binanceCommission; }
    public String     binanceAlphaApiKey()        { return binanceAlphaApiKey; }
    public String     binanceAlphaSecretKey()     { return binanceAlphaSecretKey; }
    public BigDecimal binanceAlphaCommission()    { return binanceAlphaCommission; }
    public String     okxApiKey()                 { return okxApiKey; }
    public String     okxSecretKey()              { return okxSecretKey; }
    public String     okxPassphrase()             { return okxPassphrase; }
    public BigDecimal okxCommission()             { return okxCommission; }
    public String     binanceFutApiKey()          { return binanceFutApiKey; }
    public String     binanceFutSecretKey()       { return binanceFutSecretKey; }
    public BigDecimal binanceFutCommission()      { return binanceFutCommission; }
    public boolean    binanceFutHedgeMode()       { return binanceFutHedgeMode; }
    public String     okxFutApiKey()              { return okxFutApiKey; }
    public String     okxFutSecretKey()           { return okxFutSecretKey; }
    public String     okxFutPassphrase()          { return okxFutPassphrase; }
    public BigDecimal okxFutCommission()          { return okxFutCommission; }
    public boolean    okxFutHedgeMode()           { return okxFutHedgeMode; }
    public String     binanceWsBase()             { return binanceWsBase; }
    public String     binanceRestBase()           { return binanceRestBase; }
    public String     binanceTickPath()           { return binanceTickPath; }
    public String     binanceAccountPath()        { return binanceAccountPath; }
    public String     binanceOrderPath()          { return binanceOrderPath; }
    public String     binanceAlphaWsBase()        { return binanceAlphaWsBase; }
    public String     binanceAlphaRestBase()      { return binanceAlphaRestBase; }
    public String     binanceAlphaTickPath()      { return binanceAlphaTickPath; }
    public String     binanceAlphaAccountPath()   { return binanceAlphaAccountPath; }
    public String     binanceAlphaOrderPath()     { return binanceAlphaOrderPath; }
    public String     okxWsBase()                 { return okxWsBase; }
    public String     okxRestBase()               { return okxRestBase; }
    public String     okxTickPath()               { return okxTickPath; }
    public String     okxAccountPath()            { return okxAccountPath; }
    public String     okxOrderPath()              { return okxOrderPath; }
    public String     binanceFutWsBase()          { return binanceFutWsBase; }
    public String     binanceFutRestBase()        { return binanceFutRestBase; }
    public String     binanceFutTickPath()        { return binanceFutTickPath; }
    public String     binanceFutAccountPath()     { return binanceFutAccountPath; }
    public String     binanceFutOrderPath()       { return binanceFutOrderPath; }
    public String     okxFutWsBase()              { return okxFutWsBase; }
    public String     okxFutRestBase()            { return okxFutRestBase; }
    public String     okxFutTickPath()            { return okxFutTickPath; }
    public String     okxFutAccountPath()         { return okxFutAccountPath; }
    public String     okxFutOrderPath()           { return okxFutOrderPath; }
}
