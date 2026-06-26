package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * RSI strategy operating on completed candle close prices.
 *
 * <ul>
 *   <li>BUY  when RSI drops below {@code oversold}   (default 30)</li>
 *   <li>SELL when RSI rises above {@code overbought}  (default 70)</li>
 * </ul>
 *
 * <p>Uses Wilder's smoothed RSI, standard 14-candle period.
 * On a 5m timeframe, 14 candles = 70 minutes of history.
 */
@Component("rsi") // Matches "strategyName: rsi" inside application.yml
@Scope("prototype") // State isolation for concurrent asset scanning
public class RsiStrategy implements TradingStrategy {

    private static final Logger log = LoggerFactory.getLogger(RsiStrategy.class);

    private final int        period;
    private final double     overbought;
    private final double     oversold;
    private final BigDecimal usdtAmount;

    private BigDecimal prevClose = null;
    private BigDecimal avgGain   = null;
    private BigDecimal avgLoss   = null;
    private int        count     = 0;
    private double     sumGain   = 0;
    private double     sumLoss   = 0;

    public RsiStrategy(int period, double oversold, double overbought, BigDecimal usdtAmount) {
        this.period     = period;
        this.oversold   = oversold;
        this.overbought = overbought;
        this.usdtAmount = usdtAmount;
    }

    public RsiStrategy(BigDecimal usdtAmount) {
        this(14, 30.0, 70.0, usdtAmount);
    }

//    @Override
//    public String name() {
//        return "RSI(%d, %.0f/%.0f)".formatted(period, oversold, overbought);
//    }
    @Override
    public String name() {
        return "rsi";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        return candles
            .flatMap(candle -> {
                Double rsi = computeRsi(candle.close());
                if (rsi == null) return Flux.empty();

                log.debug("[{}] Candle {} C={} RSI={}", name(), candle.openTime(), candle.close(),
                    String.format("%.2f", rsi));

                Signal.Action action = Signal.Action.HOLD;
                if      (rsi < oversold)   action = Signal.Action.BUY;
                else if (rsi > overbought) action = Signal.Action.SELL;

                if (action == Signal.Action.HOLD) return Flux.<Signal>empty();

                Signal signal = Signal.builder()
                    .action(action)
                    .symbol(symbol)
                    .usdtAmount(usdtAmount)
                    .reason("%s — RSI=%.2f".formatted(name(), rsi))
                    .timestamp(Instant.now())
                    .build();

                log.info("[{}] Signal: {} — {}", name(), action, signal.reason());
                return Flux.just(signal);
            });
    }

    private Double computeRsi(BigDecimal close) {
        if (prevClose == null) { prevClose = close; return null; }
        double change = close.subtract(prevClose).doubleValue();
        double gain   = Math.max(change, 0);
        double loss   = Math.max(-change, 0);
        prevClose = close;
        count++;

        if (count <= period) {
            sumGain += gain;
            sumLoss += loss;
            if (count < period) return null;
            avgGain = BigDecimal.valueOf(sumGain / period);
            avgLoss = BigDecimal.valueOf(sumLoss / period);
        } else {
            double a = 1.0 / period;
            avgGain  = BigDecimal.valueOf(avgGain.doubleValue() * (1 - a) + gain * a);
            avgLoss  = BigDecimal.valueOf(avgLoss.doubleValue() * (1 - a) + loss * a);
        }

        if (avgLoss.doubleValue() == 0) return 100.0;
        return 100.0 - (100.0 / (1 + avgGain.doubleValue() / avgLoss.doubleValue()));
    }
}
