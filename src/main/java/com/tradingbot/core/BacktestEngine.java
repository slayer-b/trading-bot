package com.tradingbot.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.BalanceHistory;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import com.tradingbot.strategy.TradingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Backtests any {@link TradingStrategy} against historical Binance candle data.
 *
 * <h3>Data source</h3>
 * Fetches candles from Binance REST {@code /api/v3/klines}. No API key needed —
 * this endpoint is public. Up to 1000 candles per request, auto-paginated.
 *
 * <h3>Simulation</h3>
 * <ul>
 *   <li>BUY:  spends {@code usdtAmount} at candle close price minus commission</li>
 *   <li>SELL: sells all held crypto at candle close price minus commission</li>
 *   <li>Commission is deducted from the received side (same as live trading)</li>
 * </ul>
 *
 * <h3>Report</h3>
 * After processing all candles prints:
 * <ul>
 *   <li>Total return (%)</li>
 *   <li>Win rate (profitable trades / total trades)</li>
 *   <li>Total trades, buys, sells</li>
 *   <li>Max drawdown</li>
 *   <li>Sharpe ratio (approximate, daily returns)</li>
 * </ul>
 */
public class BacktestEngine {

    private static final Logger log = LoggerFactory.getLogger(BacktestEngine.class);

    private static final String BINANCE_KLINES = "https://api.binance.com/api/v3/klines";
    private static final int    MAX_PER_PAGE   = 1000;

    private final WebClient      webClient;
    private final ObjectMapper   objectMapper;
    private final TradingStrategy strategy;
    private final String          symbol;
    private final String          interval;      // "1m", "5m", "1h" etc.
    private final int             candleCount;   // total historical candles to fetch
    private final BigDecimal      initialUsdt;
    private final BigDecimal      usdtPerTrade;
    private final BigDecimal      commissionRate;

    public BacktestEngine(WebClient webClient, ObjectMapper objectMapper,
                          TradingStrategy strategy,
                          String symbol, String interval, int candleCount,
                          BigDecimal initialUsdt, BigDecimal usdtPerTrade,
                          BigDecimal commissionRate) {
        this.webClient      = webClient;
        this.objectMapper   = objectMapper;
        this.strategy       = strategy;
        this.symbol         = symbol;
        this.interval       = interval;
        this.candleCount    = Math.min(candleCount, 10_000);
        this.initialUsdt    = initialUsdt;
        this.usdtPerTrade   = usdtPerTrade;
        this.commissionRate = commissionRate;
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public Mono<BacktestResult> run() {
        log.info("[Backtest] Starting: {} {} x{} candles | strategy: {} | initial: {} USDT",
            symbol, interval, candleCount, strategy.name(), initialUsdt);

        return fetchCandles()
            .collectList()
            .map(this::simulate);
    }

    // -------------------------------------------------------------------------
    // Fetch historical candles from Binance
    // -------------------------------------------------------------------------

    private Flux<Candle> fetchCandles() {
        // Fetch in pages of MAX_PER_PAGE, newest first then reverse
        int pages = (int) Math.ceil((double) candleCount / MAX_PER_PAGE);
        long endTime = Instant.now().toEpochMilli();
        Duration tf  = parseDuration(interval);

        return Flux.range(0, pages)
            .concatMap(page -> {
                long pageEnd   = endTime - (long) page * MAX_PER_PAGE * tf.toMillis();
                int  limit     = Math.min(MAX_PER_PAGE, candleCount - page * MAX_PER_PAGE);
                return fetchPage(pageEnd, limit);
            })
            .sort((a, b) -> a.openTime().compareTo(b.openTime())); // oldest first
    }

    private Flux<Candle> fetchPage(long endTime, int limit) {
        String url = BINANCE_KLINES + "?symbol=" + symbol
            + "&interval=" + interval
            + "&endTime=" + endTime
            + "&limit=" + limit;

        return webClient.get().uri(url)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .flatMapMany(json -> {
                List<Candle> candles = new ArrayList<>();
                Duration tf = parseDuration(interval);
                for (JsonNode row : json) {
                    // Binance kline: [openTime, open, high, low, close, volume, ...]
                    LocalDateTime openTime = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(row.get(0).asLong()), ZoneOffset.systemDefault());
                    candles.add(Candle.builder()
                        .symbol(symbol)
                        .openTime(openTime)
                        .closeTime(openTime.plus(tf))
                        .open(new BigDecimal(row.get(1).asText()))
                        .high(new BigDecimal(row.get(2).asText()))
                        .low(new BigDecimal(row.get(3).asText()))
                        .close(new BigDecimal(row.get(4).asText()))
                        .volume(new BigDecimal(row.get(5).asText()))
                        .tickCount(0)
                        .timeframe(tf)
                        .build());
                }
                return Flux.fromIterable(candles);
            })
            .doOnError(e -> log.error("[Backtest] Failed to fetch candles: {}", e.getMessage()));
    }

    // -------------------------------------------------------------------------
    // Simulate trades
    // -------------------------------------------------------------------------

    private BacktestResult simulate(List<Candle> candles) {
        if (candles.isEmpty()) {
            log.warn("[Backtest] No candles fetched — aborting");
            return BacktestResult.empty(strategy.name(), symbol, interval);
        }

        log.info("[Backtest] Simulating {} candles ({} → {})",
            candles.size(), candles.get(0).openTime(), candles.get(candles.size()-1).openTime());

        // State
        BigDecimal usdt    = initialUsdt;
        BigDecimal crypto  = BigDecimal.ZERO;
        int  buys = 0, sells = 0, wins = 0;
        BigDecimal lastBuyPrice  = null;
        BigDecimal peakPortfolio = initialUsdt;
        BigDecimal maxDrawdown   = BigDecimal.ZERO;
        List<BigDecimal> portfolioValues = new ArrayList<>();

        // Feed candles through the strategy synchronously (all candles are already known)
        List<Signal> signals = strategy.evaluate(
            Flux.fromIterable(candles),
            Mono.just(AccountState.builder()
                .balances(Map.of("USDT", initialUsdt))
                .openOrders(Map.of())
                .snapshotTime(Instant.now())
                .balanceHistory(BalanceHistory.withCapacity(candleCount))
                .build()),
            symbol
        ).collectList().block();

        if (signals == null) signals = List.of();

        // Build a map from signal index position to candle — signals come out
        // in candle order so we zip them by tracking how many we've consumed
        int sigIdx = 0;
        for (int i = 0; i < candles.size(); i++) {
            Candle candle = candles.get(i);
            BigDecimal price = candle.close();

            // Check if any signal corresponds to this candle (by matching timestamp proximity)
            while (sigIdx < signals.size()) {
                Signal sig = signals.get(sigIdx);
                // Signal timestamp should be within 1 candle of this candle's close
                if (!sig.timestamp().isAfter(
                        candle.closeTime().toInstant(ZoneOffset.UTC).plusSeconds(1))) {
                    // Apply signal
                    if (sig.action() == Signal.Action.BUY && usdt.compareTo(usdtPerTrade) >= 0) {
                        BigDecimal spent   = usdtPerTrade;
                        BigDecimal qty     = spent.divide(price, 8, RoundingMode.DOWN)
                                                  .multiply(BigDecimal.ONE.subtract(commissionRate),
                                                            MathContext.DECIMAL64);
                        usdt   = usdt.subtract(spent);
                        crypto = crypto.add(qty);
                        lastBuyPrice = price;
                        buys++;
                        log.debug("[Backtest] BUY {} {} @ {} | USDT left: {}",
                            qty.setScale(6, RoundingMode.HALF_UP), symbol, price,
                            usdt.setScale(2, RoundingMode.HALF_UP));

                    } else if (sig.action() == Signal.Action.SELL
                               && crypto.compareTo(BigDecimal.ZERO) > 0) {
                        BigDecimal received = crypto.multiply(price, MathContext.DECIMAL64)
                                                    .multiply(BigDecimal.ONE.subtract(commissionRate),
                                                              MathContext.DECIMAL64);
                        if (lastBuyPrice != null && price.compareTo(lastBuyPrice) > 0) wins++;
                        usdt   = usdt.add(received);
                        crypto = BigDecimal.ZERO;
                        sells++;
                        log.debug("[Backtest] SELL @ {} | USDT now: {}",
                            price, usdt.setScale(2, RoundingMode.HALF_UP));
                    }
                    sigIdx++;
                } else {
                    break;
                }
            }

            // Track portfolio value and drawdown
            BigDecimal portfolioValue = usdt.add(crypto.multiply(price, MathContext.DECIMAL64));
            portfolioValues.add(portfolioValue);
            if (portfolioValue.compareTo(peakPortfolio) > 0) peakPortfolio = portfolioValue;
            BigDecimal drawdown = peakPortfolio.subtract(portfolioValue)
                                               .divide(peakPortfolio, 8, RoundingMode.HALF_UP);
            if (drawdown.compareTo(maxDrawdown) > 0) maxDrawdown = drawdown;
        }

        // Liquidate remaining crypto at final price
        BigDecimal finalPrice = candles.get(candles.size() - 1).close();
        BigDecimal finalValue = usdt.add(crypto.multiply(finalPrice, MathContext.DECIMAL64));

        int totalTrades = buys + sells;
        BigDecimal returnPct = finalValue.subtract(initialUsdt)
                                         .divide(initialUsdt, 6, RoundingMode.HALF_UP)
                                         .multiply(BigDecimal.valueOf(100));
        double winRate = sells > 0 ? (double) wins / sells * 100 : 0;
        BigDecimal sharpe = computeSharpe(portfolioValues);

        BacktestResult result = new BacktestResult(
            strategy.name(), symbol, interval, candles.size(),
            candles.get(0).openTime(), candles.get(candles.size()-1).openTime(),
            initialUsdt, finalValue, returnPct,
            totalTrades, buys, sells, winRate,
            maxDrawdown.multiply(BigDecimal.valueOf(100)),
            sharpe
        );

        printReport(result);
        return result;
    }

    // -------------------------------------------------------------------------
    // Sharpe ratio (annualised, assumes each candle is one period)
    // -------------------------------------------------------------------------

    private BigDecimal computeSharpe(List<BigDecimal> values) {
        if (values.size() < 2) return BigDecimal.ZERO;
        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < values.size(); i++) {
            double prev = values.get(i - 1).doubleValue();
            double curr = values.get(i).doubleValue();
            if (prev > 0) returns.add((curr - prev) / prev);
        }
        double mean = returns.stream().mapToDouble(d -> d).average().orElse(0);
        double var  = returns.stream().mapToDouble(d -> (d - mean) * (d - mean)).average().orElse(0);
        double std  = Math.sqrt(var);
        if (std == 0) return BigDecimal.ZERO;
        // Annualise: multiply by sqrt(periodsPerYear)
        double periodsPerYear = 365.0 * 24.0 * 60.0 / parseDuration(interval).toMinutes();
        return BigDecimal.valueOf(mean / std * Math.sqrt(periodsPerYear)).setScale(3, RoundingMode.HALF_UP);
    }

    // -------------------------------------------------------------------------
    // Report
    // -------------------------------------------------------------------------

    private void printReport(BacktestResult r) {
        log.info("");
        log.info("╔══════════════════════════════════════════════════════╗");
        log.info("║              BACKTEST REPORT                         ║");
        log.info("╠══════════════════════════════════════════════════════╣");
        log.info("║  Strategy   : {}", r.strategyName());
        log.info("║  Symbol     : {}  Interval: {}  Candles: {}",
            r.symbol(), r.interval(), r.candleCount());
        log.info("║  Period     : {} → {}", r.fromTime(), r.toTime());
        log.info("╠══════════════════════════════════════════════════════╣");
        log.info("║  Initial    : {} USDT", r.initialUsdt().setScale(2, RoundingMode.HALF_UP));
        log.info("║  Final      : {} USDT", r.finalUsdt().setScale(2, RoundingMode.HALF_UP));
        log.info("║  Return     : {}%", r.returnPct().setScale(2, RoundingMode.HALF_UP));
        log.info("╠══════════════════════════════════════════════════════╣");
        log.info("║  Trades     : {}  (buys: {}  sells: {})", r.totalTrades(), r.buys(), r.sells());
        log.info("║  Win rate   : {}%", String.format("%.1f", r.winRate()));
        log.info("║  Max DD     : {}%", r.maxDrawdownPct().setScale(2, RoundingMode.HALF_UP));
        log.info("║  Sharpe     : {}", r.sharpeRatio());
        log.info("╚══════════════════════════════════════════════════════╝");
        log.info("");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Duration parseDuration(String interval) {
        if (interval.endsWith("m")) return Duration.ofMinutes(Long.parseLong(interval.replace("m", "")));
        if (interval.endsWith("h")) return Duration.ofHours(Long.parseLong(interval.replace("h", "")));
        if (interval.endsWith("d")) return Duration.ofDays(Long.parseLong(interval.replace("d", "")));
        return Duration.ofMinutes(1);
    }

    // -------------------------------------------------------------------------
    // Result record
    // -------------------------------------------------------------------------

    public record BacktestResult(
        String        strategyName,
        String        symbol,
        String        interval,
        int           candleCount,
        LocalDateTime fromTime,
        LocalDateTime toTime,
        BigDecimal    initialUsdt,
        BigDecimal    finalUsdt,
        BigDecimal    returnPct,
        int           totalTrades,
        int           buys,
        int           sells,
        double        winRate,
        BigDecimal    maxDrawdownPct,
        BigDecimal    sharpeRatio
    ) {
        static BacktestResult empty(String strategy, String symbol, String interval) {
            return new BacktestResult(strategy, symbol, interval, 0,
                LocalDateTime.now(), LocalDateTime.now(),
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                0, 0, 0, 0, BigDecimal.ZERO, BigDecimal.ZERO);
        }
    }
}
