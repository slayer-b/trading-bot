package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Decorator that wraps any {@link TradingStrategy} and suppresses signals
 * when the triggering candle's volume is below a rolling average threshold.
 *
 * <h3>How it works</h3>
 * On every candle the filter updates its rolling volume average, then passes
 * the candle to the inner strategy. Signals are only forwarded when
 * {@code candle.volume >= rollingAvg * threshold}.
 *
 * <p>Key design: we update volume state inside a single {@code .map()} on the
 * candle stream before handing it to the inner strategy. This guarantees the
 * volume average is always current when the inner strategy emits a signal for
 * that candle — no parallel subscriptions, no autoConnect races.
 */
public class VolumeFilter implements TradingStrategy {

    private static final Logger log = LoggerFactory.getLogger(VolumeFilter.class);

    private final TradingStrategy inner;
    private final int             lookback;
    private final double          threshold;

    private final Deque<BigDecimal> window = new ArrayDeque<>();
    private BigDecimal rollingSum  = BigDecimal.ZERO;
    private BigDecimal lastVolume  = BigDecimal.ZERO;
    private boolean    warmedUp    = false;

    public VolumeFilter(TradingStrategy inner, int lookback, double threshold) {
        if (lookback <= 0)    throw new IllegalArgumentException("lookback must be > 0");
        if (threshold <= 0.0) throw new IllegalArgumentException("threshold must be > 0");
        this.inner     = inner;
        this.lookback  = lookback;
        this.threshold = threshold;
    }

    @Override
    public String name() {
        return inner.name() + "+Vol(" + lookback + "×" + threshold + ")";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        // Update volume state on every candle BEFORE the inner strategy sees it.
        // A single .map() call is sequential — no dual-subscription issues.
        Flux<Candle> instrumented = candles.map(candle -> {
            updateVolume(candle.volume());
            return candle;
        });

        return inner.evaluate(instrumented, accountState, symbol)
            .filter(signal -> {
                if (!warmedUp) {
                    log.debug("[{}] Volume warm-up {}/{}", name(), window.size(), lookback);
                    return false;
                }
                BigDecimal avg      = rollingAverage();
                BigDecimal required = avg.multiply(BigDecimal.valueOf(threshold), MathContext.DECIMAL64);
                boolean    pass     = lastVolume.compareTo(required) >= 0;
                if (pass) {
                    log.debug("[{}] Volume confirmed: {} >= {} (avg {} × {})",
                        name(),
                        lastVolume.setScale(2, RoundingMode.HALF_UP),
                        required.setScale(2, RoundingMode.HALF_UP),
                        avg.setScale(2, RoundingMode.HALF_UP),
                        threshold);
                } else {
                    log.info("[{}] {} filtered — low volume: {} < {} (avg {} × {})",
                        name(), signal.action(),
                        lastVolume.setScale(2, RoundingMode.HALF_UP),
                        required.setScale(2, RoundingMode.HALF_UP),
                        avg.setScale(2, RoundingMode.HALF_UP),
                        threshold);
                }
                return pass;
            });
    }

    // -------------------------------------------------------------------------
    // Rolling volume state — called sequentially from the candle map()
    // -------------------------------------------------------------------------

    private void updateVolume(BigDecimal volume) {
        lastVolume = volume;
        window.addLast(volume);
        rollingSum = rollingSum.add(volume);
        if (window.size() > lookback) {
            rollingSum = rollingSum.subtract(window.removeFirst());
        }
        warmedUp = window.size() >= lookback;
    }

    private BigDecimal rollingAverage() {
        if (window.isEmpty()) return BigDecimal.ZERO;
        return rollingSum.divide(
            BigDecimal.valueOf(window.size()), 8, RoundingMode.HALF_UP);
    }
}
