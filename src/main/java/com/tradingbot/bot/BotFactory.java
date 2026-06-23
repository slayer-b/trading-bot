package com.tradingbot.bot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.account.AccountService;
import com.tradingbot.core.CandleAggregator;
import com.tradingbot.core.TradingEngine;
import com.tradingbot.market.*;
import com.tradingbot.market.config.MarketUrls;
import com.tradingbot.strategy.*;
import com.tradingbot.strategy.LstmStrategy;
import com.tradingbot.strategy.OrderBookImbalanceStrategy;
import com.tradingbot.strategy.VolumeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

/**
 * Creates a fully wired {@link TradingEngine} from a {@link BotConfig}.
 * Each call produces an independent, isolated bot instance.
 */
public class BotFactory {

    private static final Logger log = LoggerFactory.getLogger(BotFactory.class);

    private final WebClient        webClient;
    private final ObjectMapper     objectMapper;
    private final ApplicationContext appContext;

    public BotFactory(WebClient webClient, ObjectMapper objectMapper,
                      ApplicationContext appContext) {
        this.webClient    = webClient;
        this.objectMapper = objectMapper;
        this.appContext   = appContext;
    }

    public TradingEngine create(BotConfig cfg) {
        log.info("[BotFactory] Creating bot '{}': {} {} {} {}m candles{}",
            cfg.name(), cfg.market(), cfg.symbol(), cfg.strategyName(),
            cfg.candleTimeframeMinutes(),
            cfg.volumeFilterLookback() > 0
                ? " +VolumeFilter(lookback=" + cfg.volumeFilterLookback()
                  + " threshold=" + cfg.volumeFilterThreshold() + ")" : "");

        Market          market           = buildMarket(cfg);
        TradingStrategy baseStrategy     = buildStrategy(cfg);
        TradingStrategy strategy         = applyFilters(cfg, baseStrategy);
        AccountService  accountService   = new AccountService(market, cfg.historyCapacity(), cfg.quoteAsset());
        CandleAggregator candleAggregator = new CandleAggregator(Duration.ofMinutes(cfg.candleTimeframeMinutes()));

        return new TradingEngine(market, strategy, candleAggregator,
                                 accountService, appContext, cfg.symbol(), cfg.quoteAsset(), cfg.name());
    }

    private TradingStrategy applyFilters(BotConfig cfg, TradingStrategy strategy) {
        if (cfg.volumeFilterLookback() > 0) {
            return new VolumeFilter(strategy, cfg.volumeFilterLookback(), cfg.volumeFilterThreshold());
        }
        return strategy;
    }

    // -------------------------------------------------------------------------
    // Market builder
    // -------------------------------------------------------------------------

    private Market buildMarket(BotConfig c) {
        return switch (c.market().toLowerCase()) {

            case "binance" ->
                new BinanceMarket(webClient, objectMapper,
                    urls(c.binanceWsBase(), c.binanceRestBase(), c.binanceTickPath(),
                         c.binanceAccountPath(), c.binanceOrderPath()),
                    "Binance[" + c.name() + "]",
                    c.binanceApiKey(), c.binanceSecretKey(), c.binanceCommission());

            case "binance-notrade" ->
                new NoTradeMarket(objectMapper,
                    urls(c.binanceWsBase(), c.binanceRestBase(), c.binanceTickPath(),
                         c.binanceAccountPath(), c.binanceOrderPath()),
                    "Binance(paper)[" + c.name() + "]",
                    c.initialUsdt(), c.binanceCommission());

            case "binance-alpha" ->
                new BinanceMarket(webClient, objectMapper,
                    urls(c.binanceAlphaWsBase(), c.binanceAlphaRestBase(), c.binanceAlphaTickPath(),
                         c.binanceAlphaAccountPath(), c.binanceAlphaOrderPath()),
                    "BinanceAlpha[" + c.name() + "]",
                    c.binanceAlphaApiKey(), c.binanceAlphaSecretKey(), c.binanceAlphaCommission());

            case "binance-alpha-notrade" ->
                new NoTradeMarket(objectMapper,
                    urls(c.binanceAlphaWsBase(), c.binanceAlphaRestBase(), c.binanceAlphaTickPath(),
                         c.binanceAlphaAccountPath(), c.binanceAlphaOrderPath()),
                    "BinanceAlpha(paper)[" + c.name() + "]",
                    c.initialUsdt(), c.binanceAlphaCommission());

            case "okx" ->
                new OkxMarket(webClient, objectMapper,
                    urls(c.okxWsBase(), c.okxRestBase(), c.okxTickPath(),
                         c.okxAccountPath(), c.okxOrderPath()),
                    c.okxApiKey(), c.okxSecretKey(), c.okxPassphrase(), c.okxCommission());

            case "okx-notrade" ->
                new NoTradeMarket(objectMapper,
                    urls(c.okxWsBase(), c.okxRestBase(), c.okxTickPath(),
                         c.okxAccountPath(), c.okxOrderPath()),
                    "OKX(paper)[" + c.name() + "]",
                    c.initialUsdt(), c.okxCommission());

            case "binance-futures" -> {
                var m = new BinanceFuturesMarket(webClient, objectMapper,
                    urls(c.binanceFutWsBase(), c.binanceFutRestBase(), c.binanceFutTickPath(),
                         c.binanceFutAccountPath(), c.binanceFutOrderPath()),
                    "BinanceFutures[" + c.name() + "]",
                    c.binanceFutApiKey(), c.binanceFutSecretKey(), c.binanceFutCommission());
                m.setPositionMode(c.binanceFutHedgeMode()).block();
                yield m;
            }

            case "binance-futures-notrade" ->
                new NoTradeFuturesMarket(objectMapper,
                    urls(c.binanceFutWsBase(), c.binanceFutRestBase(), c.binanceFutTickPath(),
                         c.binanceFutAccountPath(), c.binanceFutOrderPath()),
                    "BinanceFutures(paper)[" + c.name() + "]",
                    c.initialUsdt(), c.binanceFutCommission());

            case "okx-futures" -> {
                var m = new OkxFuturesMarket(webClient, objectMapper,
                    urls(c.okxFutWsBase(), c.okxFutRestBase(), c.okxFutTickPath(),
                         c.okxFutAccountPath(), c.okxFutOrderPath()),
                    c.okxFutApiKey(), c.okxFutSecretKey(), c.okxFutPassphrase(),
                    c.okxFutCommission());
                m.setPositionMode(c.okxFutHedgeMode()).block();
                yield m;
            }

            case "okx-futures-notrade" ->
                new NoTradeFuturesMarket(objectMapper,
                    urls(c.okxFutWsBase(), c.okxFutRestBase(), c.okxFutTickPath(),
                         c.okxFutAccountPath(), c.okxFutOrderPath()),
                    "OKXFutures(paper)[" + c.name() + "]",
                    c.initialUsdt(), c.okxFutCommission());

            default -> throw new IllegalArgumentException(
                "Unknown market: '" + c.market() + "' in bot '" + c.name() + "'");
        };
    }

    // -------------------------------------------------------------------------
    // Strategy builder
    // -------------------------------------------------------------------------

    private TradingStrategy buildStrategy(BotConfig c) {
        return switch (c.strategyName().toLowerCase()) {
            case "breakout_retest_daily_15m_5m" -> new BreakoutRetestStrategy();
            case "grid-ema-trend"      -> new GridEmaTradingStrategy(10.0, 10.0, 5, 5, "BNBUSDT");
            case "ema"                 -> new MovingAverageCrossStrategy(c.emaFast(), c.emaSlow(), c.usdtAmount());
            case "rsi"                 -> new RsiStrategy(c.rsiPeriod(), c.rsiOversold(), c.rsiOverbought(), c.usdtAmount());
            case "macd"                -> new MacdStrategy(c.macdFast(), c.macdSlow(), c.macdSignal(), c.usdtAmount());
            case "bet"                 -> new BetTradingAlgorithm();
            case "emarsi"              -> new EmaRsiCombinedStrategy(
                                             c.emaRsiFast(), c.emaRsiSlow(),
                                             c.emaRsiRsiPeriod(), c.emaRsiOversold(), c.emaRsiOverbought(),
                                             c.usdtAmount());
            case "orderbookimbalance"  -> new OrderBookImbalanceStrategy(
                                             c.obDepth(), c.obBuyThreshold(), c.obSellThreshold(),
                                             c.obConfirmationTicks(), c.obMinSpreadBps(),
                                             c.usdtAmount());
            case "lstm"               -> new LstmStrategy(
                                             c.lstmLookback(), c.lstmMinTrainCandles(),
                                             c.lstmRetrainEvery(), c.lstmBuyThreshold(),
                                             c.lstmSellThreshold(), c.usdtAmount());
            case "low-price"          -> new LowPriceChangeStrategy();
            default -> throw new IllegalArgumentException(
                "Unknown strategy: '" + c.strategyName() + "' in bot '" + c.name() +
                "'. Valid: ema, rsi, macd, emaRsi, orderBookImbalance, lstm");
        };
    }

    private MarketUrls urls(String ws, String rest, String tick, String account, String order) {
        return new MarketUrls(ws, rest, tick, account, order);
    }
}
