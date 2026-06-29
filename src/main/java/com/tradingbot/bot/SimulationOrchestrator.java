package com.tradingbot.bot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.core.TradingEngine;
import com.tradingbot.market.Market;
import com.tradingbot.service.HistoryService;
import com.tradingbot.strategy.BreakoutRetestStrategy;
import com.tradingbot.strategy.TradingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.yaml.snakeyaml.Yaml;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Loads all yml files from {@code classpath:current/*.yml}, creates one
 * {@link TradingEngine} per file, runs them all in parallel, stops them
 * after {@code trading.simulation.durationMinutes}, then prints a ranked
 * comparison table.
 *
 * <p>Each bot inherits all values from {@code common.yml} and overrides
 * only what its own yml specifies.
 */
@Component
public class SimulationOrchestrator {

    @Autowired
    private HistoryService historyService;
    @Autowired
    private BotFactory botFactory;

    private static final Logger log = LoggerFactory.getLogger(SimulationOrchestrator.class);

    private final WebClient          webClient;
    private final ObjectMapper       objectMapper;
    private final ApplicationContext appContext;
    private final CommonConfig       common;

    public SimulationOrchestrator(WebClient webClient, ObjectMapper objectMapper,
                                   ApplicationContext appContext, CommonConfig common) {
        this.webClient    = webClient;
        this.objectMapper = objectMapper;
        this.appContext   = appContext;
        this.common       = common;
    }

    // -------------------------------------------------------------------------
    // Entry point — called from Spring ApplicationReadyEvent
    // -------------------------------------------------------------------------

    public void run() {
        List<BotConfig> configs = loadBotConfigs();

        if (configs.isEmpty()) {
            log.warn("[Orchestrator] No bot yml files found in simulation/ or trade/. Nothing to run.");
            return;
        }

        log.info("[Orchestrator] Starting {} bot(s) for {} minutes",
            configs.size(), common.simulationDurationMinutes());

        List<TradingEngine> engines = configs.stream()
                .map((BotConfig cfg) -> botFactory.createTradingEngine(cfg, historyService))
            .toList();

        // Start all bots in parallel
        Flux.fromIterable(engines)
            .flatMap(TradingEngine::start)
            .subscribe(
                v  -> {},
                e  -> log.error("[Orchestrator] Bot startup error: {}", e.getMessage(), e)
            );

        // Schedule stop + report after simulation duration.
        // IMPORTANT: Mono.delay runs on a parallel scheduler thread — calling
        // SpringApplication.exit() from there triggers block() inside Spring's
        // reactive context shutdown, causing IllegalStateException.
        // Fix: hand off to a plain daemon thread before calling exit().
        int durationMin = common.simulationDurationMinutes();
        if (durationMin > 0) {
            Mono.delay(Duration.ofMinutes(durationMin))
                .subscribe(t -> {
                    // Jump off the Reactor thread before shutting down Spring
                    Thread shutdownThread = new Thread(() -> stopAndReport(engines),
                        "simulation-shutdown");
                    shutdownThread.setDaemon(false);
                    shutdownThread.start();
                });
        } else {
            log.info("[Orchestrator] durationMinutes=0 — running indefinitely (live mode)");
        }
    }

    // -------------------------------------------------------------------------
    // Stop all bots and print report
    // -------------------------------------------------------------------------

    private void stopAndReport(List<TradingEngine> engines) {
        log.info("[Orchestrator] Simulation time elapsed — stopping all bots...");

        List<BotResult> results = engines.stream()
            .map(e -> {
                BotResult r = e.buildResult();
                e.stop();
                return r;
            })
            .sorted(Comparator.comparing(BotResult::returnPct).reversed())
            .toList();

        printComparisonTable(results);

        // Exit after report if this was a pure simulation (not live)
        if (common.simulationDurationMinutes() > 0) {
            int code = SpringApplication.exit(appContext, () -> 0);
            System.exit(code);
        }
    }

    // -------------------------------------------------------------------------
    // Comparison table
    // -------------------------------------------------------------------------

    private void printComparisonTable(List<BotResult> results) {
        log.info("");
        log.info("╔══════════════════════════════════════════════════════════════════════════════════════╗");
        log.info("║                        SIMULATION RESULTS — RANKED BY RETURN                        ║");
        log.info("╠═══════════════════╦══════════╦══════════╦════════╦══════════╦═════════╦══════════╦══╣");
        log.info("║ Bot               ║ Symbol   ║ Strategy ║ TF(m)  ║ Return % ║ WinRate ║ Trades   ║  ║");
        log.info("╠═══════════════════╬══════════╬══════════╬════════╬══════════╬═════════╬══════════╬══╣");

        for (int i = 0; i < results.size(); i++) {
            BotResult r   = results.get(i);
            String medal  = i == 0 ? "1st" : i == 1 ? "2nd" : i == 2 ? "3rd" : "   ";
            String returnStr = (r.returnPct().compareTo(BigDecimal.ZERO) >= 0 ? "+" : "")
                + r.returnPct() + "%";
            log.info("║ {} ║",
                String.format("%-17s ║ %-8s ║ %-8s ║ %-6d ║ %-8s ║ %-7s ║ %-8d ║ %s",
                    truncate(r.name(), 17),
                    truncate(r.symbol(), 8),
                    truncate(r.strategy(), 8),
                    r.candleTimeframeMinutes(),
                    returnStr,
                    String.format("%.1f%%", r.winRate()),
                    r.totalTrades(),
                    medal));
        }

        log.info("╠═══════════════════╩══════════╩══════════╩════════╩══════════╩═════════╩══════════╩══╣");
        log.info("║ DETAIL                                                                               ║");
        log.info("╠══════════════════════════════════════════════════════════════════════════════════════╣");

        for (BotResult r : results) {
            log.info("║  [{}]", r.name());
            log.info("║    Initial: {} USDT → Final: {} USDT  ({}{})",
                r.initialUsdt().setScale(2, RoundingMode.HALF_UP),
                r.finalUsdt().setScale(2, RoundingMode.HALF_UP),
                r.profitUsdt().compareTo(BigDecimal.ZERO) >= 0 ? "+" : "",
                r.profitUsdt());
            log.info("║    Trades: {}  Wins: {}  Losses: {}  Win rate: {}%",
                r.totalTrades(), r.wins(), r.losses(),
                String.format("%.1f", r.winRate()));
            log.info("║    Max drawdown: {}%  Commission paid: {} USDT",
                r.maxDrawdownPct(), r.commissionPaid());
        }

        log.info("╚══════════════════════════════════════════════════════════════════════════════════════╝");
        log.info("");
    }

    // -------------------------------------------------------------------------
    // Load bot ymls
    // -------------------------------------------------------------------------

    /**
     * Scans {@code classpath:current/*.yml}, parses each with SnakeYAML,
     * and merges with common config to produce a {@link BotConfig}.
     */
    private List<BotConfig> loadBotConfigs() {
        List<BotConfig> configs = new ArrayList<>();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        java.util.Set<String> usedNames = new java.util.HashSet<>();

        try {
            Resource[] simResources   = resolver.getResources("classpath:simulation/*.yml");
//            Resource[] tradeResources = resolver.getResources("classpath:trade/*.yml");
            Resource[] resources = simResources;
//                        java.util.Arrays.stream(tradeResources))
            // Sort by filename for deterministic ordering
            java.util.Arrays.sort(resources,
                Comparator.comparing(r -> r.getFilename() != null ? r.getFilename() : ""));
            Yaml yaml = new Yaml();

            for (Resource res : resources) {
                String filename = res.getFilename();
                if (filename == null) continue;
                String fileBase = filename.replace(".yml", "");

                try (InputStream in = res.getInputStream()) {
                    Map<String, Object> raw = yaml.load(in);
                    BotConfig cfg = merge(fileBase, raw);

                    // Ensure unique name — if duplicate, append filename
                    String name = cfg.name();
                    if (!usedNames.add(name)) {
                        name = name + "-" + fileBase;
                        log.warn("[Orchestrator] Duplicate bot name '{}', renamed to '{}'",
                            cfg.name(), name);
                        // Rebuild with new name
                        cfg = merge(fileBase, raw, name);
                    }

                    configs.add(cfg);
                    log.info("[Orchestrator] Loaded bot: {} ({} {} {}m)",
                        cfg.name(), cfg.symbol(), cfg.strategyName(), cfg.candleTimeframeMinutes());
                } catch (Exception e) {
                    log.error("[Orchestrator] Failed to load {}: {}", filename, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("[Orchestrator] Failed to scan current/*.yml: {}", e.getMessage());
        }

        return configs;
    }

    /**
     * Merges a bot's raw yml map with common config defaults.
     * Bot yml values take precedence over common.yml.
     */
    private BotConfig merge(String filename, Map<String, Object> raw) {
        // Resolve name from bot.name or fall back to filename
        String name = filename;
        if (raw.containsKey("bot")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> botSection = (Map<String, Object>) raw.get("bot");
            if (botSection != null && botSection.containsKey("name"))
                name = (String) botSection.get("name");
        }
        return merge(filename, raw, name);
    }

    @SuppressWarnings("unchecked")
    private BotConfig merge(String filename, Map<String, Object> raw, String name) {

        Map<String, Object> t = raw.containsKey("trading")
            ? (Map<String, Object>) raw.get("trading") : Map.of();

        // Volume filter sub-section
        Map<String, Object> volFilter = submap(t, "volumeFilter");
        Map<String, Object> obParams  = submap(submap(t, "strategyParams"), "orderBookImbalance");
        Map<String, Object> lstmParams = submap(submap(t, "strategyParams"), "lstm");

        // Strategy sub-sections — all camelCase
        Map<String, Object> strat      = submap(t, "strategyParams");
        Map<String, Object> emaParams  = submap(strat, "ema");
        Map<String, Object> rsiParams  = submap(strat, "rsi");
        Map<String, Object> macdParams = submap(strat, "macd");
        Map<String, Object> erParams   = submap(strat, "emaRsi");

        return new BotConfig(
            name,
            // market
            str(t, "market",                  common.market()),
            str(t, "symbol",                  common.symbol()),
            str(submap(t, "account"), "quoteAsset", common.quoteAsset()),
            dec(submap(t, "simulation"), "initialUsdt", common.initialUsdt()),
            dec(submap(t, "simulation"), "usdtAmount",  common.usdtAmount()),
            intVal(t, "candleTimeframeMinutes", common.candleTimeframeMinutes()),
            // strategy name
            str(t, "strategyName", common.strategyName()),
            // volume filter
            intVal(volFilter, "lookback",   common.volumeFilterLookback()),
            dbl(volFilter,   "threshold",   common.volumeFilterThreshold()),
            // order book imbalance
            intVal(obParams, "depth",             common.obDepth()),
            dbl(obParams,   "buyThreshold",       common.obBuyThreshold()),
            dbl(obParams,   "sellThreshold",      common.obSellThreshold()),
            intVal(obParams, "confirmationTicks",  common.obConfirmationTicks()),
            dbl(obParams,   "minSpreadBps",        common.obMinSpreadBps()),
            // lstm
            intVal(lstmParams, "lookback",        common.lstmLookback()),
            intVal(lstmParams, "minTrainCandles",  common.lstmMinTrainCandles()),
            intVal(lstmParams, "retrainEvery",     common.lstmRetrainEvery()),
            dbl(lstmParams,   "buyThreshold",      common.lstmBuyThreshold()),
            dbl(lstmParams,   "sellThreshold",     common.lstmSellThreshold()),
            // ema
            intVal(emaParams, "fastPeriod", common.emaFast()),
            intVal(emaParams, "slowPeriod", common.emaSlow()),
            // rsi
            intVal(rsiParams, "period",      common.rsiPeriod()),
            dbl(rsiParams,   "oversold",     common.rsiOversold()),
            dbl(rsiParams,   "overbought",   common.rsiOverbought()),
            // macd
            intVal(macdParams, "fastPeriod",   common.macdFast()),
            intVal(macdParams, "slowPeriod",   common.macdSlow()),
            intVal(macdParams, "signalPeriod", common.macdSignal()),
            // ema-rsi
            intVal(erParams, "emaFastPeriod", common.emaRsiFast()),
            intVal(erParams, "emaSlowPeriod", common.emaRsiSlow()),
            intVal(erParams, "rsiPeriod",     common.emaRsiRsiPeriod()),
            dbl(erParams,   "oversold",       common.emaRsiOversold()),
            dbl(erParams,   "overbought",     common.emaRsiOverbought()),
            // account
            intVal(submap(t, "account"), "historyCapacity", common.historyCapacity()),
            // binance
            common.binanceApiKey(), common.binanceSecretKey(), common.binanceCommission(),
            // binance-alpha
            common.binanceAlphaApiKey(), common.binanceAlphaSecretKey(), common.binanceAlphaCommission(),
            // okx
            common.okxApiKey(), common.okxSecretKey(), common.okxPassphrase(), common.okxCommission(),
            // binance futures
            common.binanceFutApiKey(), common.binanceFutSecretKey(), common.binanceFutCommission(), common.binanceFutHedgeMode(),
            // okx futures
            common.okxFutApiKey(), common.okxFutSecretKey(), common.okxFutPassphrase(), common.okxFutCommission(), common.okxFutHedgeMode(),
            // urls (always from common)
            common.binanceWsBase(), common.binanceRestBase(), common.binanceTickPath(), common.binanceAccountPath(), common.binanceOrderPath(),
            common.binanceAlphaWsBase(), common.binanceAlphaRestBase(), common.binanceAlphaTickPath(), common.binanceAlphaAccountPath(), common.binanceAlphaOrderPath(),
            common.okxWsBase(), common.okxRestBase(), common.okxTickPath(), common.okxAccountPath(), common.okxOrderPath(),
            common.binanceFutWsBase(), common.binanceFutRestBase(), common.binanceFutTickPath(), common.binanceFutAccountPath(), common.binanceFutOrderPath(),
            common.okxFutWsBase(), common.okxFutRestBase(), common.okxFutTickPath(), common.okxFutAccountPath(), common.okxFutOrderPath()
        );
    }

    // -------------------------------------------------------------------------
    // Yaml helpers
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> submap(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return (v instanceof Map) ? (Map<String, Object>) v : Map.of();
    }

    private String str(Map<String, Object> m, String key, String def) {
        Object v = m.get(key);
        return v != null ? v.toString() : def;
    }

    private int intVal(Map<String, Object> m, String key, int def) {
        Object v = m.get(key);
        return v != null ? ((Number) v).intValue() : def;
    }

    private double dbl(Map<String, Object> m, String key, double def) {
        Object v = m.get(key);
        return v != null ? ((Number) v).doubleValue() : def;
    }

    private BigDecimal dec(Map<String, Object> m, String key, BigDecimal def) {
        Object v = m.get(key);
        return v != null ? new BigDecimal(v.toString()) : def;
    }

    private String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }
}
