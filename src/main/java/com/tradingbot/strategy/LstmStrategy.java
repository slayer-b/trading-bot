package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.Signal;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * LSTM (Long Short-Term Memory) neural network strategy.
 *
 * <h3>Architecture</h3>
 * <pre>
 *   Input:  sequence of {@code lookback} candles × 5 features each
 *   Layer1: LSTM(64 units)
 *   Layer2: LSTM(32 units)
 *   Output: softmax(2) — probability of [DOWN, UP] on next candle
 * </pre>
 *
 * <h3>Features per candle (all normalised 0-1 within the window)</h3>
 * <ol>
 *   <li>Normalised close price (relative to window min/max)</li>
 *   <li>Normalised volume</li>
 *   <li>RSI(14) / 100</li>
 *   <li>EMA(9)-EMA(21) spread, normalised</li>
 *   <li>Candle return: (close-open)/open</li>
 * </ol>
 *
 * <h3>Trading rules</h3>
 * <ul>
 *   <li>BUY  when P(UP) > {@code buyThreshold}  (default 0.60)</li>
 *   <li>SELL when P(UP) < {@code sellThreshold} (default 0.40)</li>
 * </ul>
 *
 * <h3>Training</h3>
 * The model trains from scratch once {@code minTrainCandles} have been collected,
 * then retrains every {@code retrainEveryCandles} to adapt to new market regimes.
 * Labels are computed from future candle direction (supervised learning on past data).
 */
public class LstmStrategy implements TradingStrategy {

    private static final Logger log = LoggerFactory.getLogger(LstmStrategy.class);

    private static final int NUM_FEATURES = 5;

    private final int        lookback;           // sequence length fed to LSTM
    private final int        minTrainCandles;    // candles needed before first train
    private final int        retrainEveryCandles;
    private final double     buyThreshold;
    private final double     sellThreshold;
    private final BigDecimal usdtAmount;

    // Candle history
    private final List<Candle> history = new ArrayList<>();
    private int candlesSinceRetrain    = 0;

    // LSTM model — null until first training
    private MultiLayerNetwork model = null;

    // EMA state
    private double fastEma = 0, slowEma = 0;
    // RSI state
    private double prevClose = -1, avgGain = 0, avgLoss = 0;
    private int    rsiCount  = 0, rsiSumCount = 0;
    private double rsiSumGain = 0, rsiSumLoss = 0;

    public LstmStrategy(int lookback, int minTrainCandles, int retrainEveryCandles,
                        double buyThreshold, double sellThreshold, BigDecimal usdtAmount) {
        this.lookback            = lookback;
        this.minTrainCandles     = minTrainCandles;
        this.retrainEveryCandles = retrainEveryCandles;
        this.buyThreshold        = buyThreshold;
        this.sellThreshold       = sellThreshold;
        this.usdtAmount          = usdtAmount;
    }

    /** Defaults: lookback=30, train after 60 candles, retrain every 20 */
    public LstmStrategy(BigDecimal usdtAmount) {
        this(30, 60, 20, 0.60, 0.40, usdtAmount);
    }

    @Override
    public String name() {
        return "LSTM(%d,retrain=%d)".formatted(lookback, retrainEveryCandles);
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState,
                                  String symbol) {
        return candles.flatMap(candle -> {
            history.add(candle);
            updateIndicators(candle);
            candlesSinceRetrain++;

            // Not enough data yet
            if (history.size() < minTrainCandles) {
                log.debug("[{}] Warming up: {}/{}", name(), history.size(), minTrainCandles);
                return Flux.empty();
            }

            // Train/retrain
            if (model == null || candlesSinceRetrain >= retrainEveryCandles) {
                trainModel();
                candlesSinceRetrain = 0;
            }

            // Need at least lookback candles to form a sequence
            if (history.size() < lookback + 1) return Flux.empty();

            // Predict
            double pUp = predict();
            log.debug("[{}] P(UP)={:.4f}", name(), pUp);

            Signal.Action action = Signal.Action.HOLD;
            if      (pUp > buyThreshold)  action = Signal.Action.BUY;
            else if (pUp < sellThreshold) action = Signal.Action.SELL;

            if (action == Signal.Action.HOLD) return Flux.<Signal>empty();

            String reason = "%s — P(UP)=%.4f".formatted(name(), pUp);
            log.info("[{}] Signal: {} — {}", name(), action, reason);

            return Flux.just(Signal.builder()
                .action(action).symbol(symbol).usdtAmount(usdtAmount)
                .reason(reason).timestamp(Instant.now())
                .build());
        });
    }

    // -------------------------------------------------------------------------
    // Neural network training
    // -------------------------------------------------------------------------

    private void trainModel() {
        int n = history.size();
        // Need lookback candles as input + 1 for the label
        int samples = n - lookback - 1;
        if (samples <= 0) return;

        log.info("[{}] Training on {} samples...", name(), samples);

        // Build training data: [samples, features, timeSteps]
        INDArray input  = Nd4j.zeros(samples, NUM_FEATURES, lookback);
        INDArray labels = Nd4j.zeros(samples, 2, lookback);

        for (int i = 0; i < samples; i++) {
            double[] closes = new double[lookback];
            double[] volumes = new double[lookback];

            // Collect raw values for normalisation
            for (int t = 0; t < lookback; t++) {
                closes[t]  = history.get(i + t).close().doubleValue();
                volumes[t] = history.get(i + t).volume().doubleValue();
            }

            double minC = min(closes), maxC = max(closes);
            double minV = min(volumes), maxV = max(volumes);
            double rangeC = maxC - minC == 0 ? 1 : maxC - minC;
            double rangeV = maxV - minV == 0 ? 1 : maxV - minV;

            for (int t = 0; t < lookback; t++) {
                Candle c = history.get(i + t);
                double normClose  = (closes[t] - minC) / rangeC;
                double normVol    = (volumes[t] - minV) / rangeV;
                double rsi        = computeRsiForCandle(i + t);
                double emaspread  = computeEmaSpreadForCandle(i + t, minC, rangeC);
                double ret        = c.open().doubleValue() == 0 ? 0
                    : (c.close().doubleValue() - c.open().doubleValue()) / c.open().doubleValue();

                input.putScalar(new int[]{i, 0, t}, normClose);
                input.putScalar(new int[]{i, 1, t}, normVol);
                input.putScalar(new int[]{i, 2, t}, Math.max(0, Math.min(1, rsi / 100.0)));
                input.putScalar(new int[]{i, 3, t}, Math.max(-1, Math.min(1, emaspread)));
                input.putScalar(new int[]{i, 4, t}, Math.max(-0.1, Math.min(0.1, ret)));
            }

            // Label: did price go UP in the next candle?
            double nextClose = history.get(i + lookback).close().doubleValue();
            double thisClose = history.get(i + lookback - 1).close().doubleValue();
            int up = nextClose > thisClose ? 1 : 0;
            labels.putScalar(new int[]{i, up, lookback - 1}, 1.0);
        }

        if (model == null) model = buildModel();

        model.fit(new org.nd4j.linalg.dataset.DataSet(input, labels));
        log.info("[{}] Training complete", name());
    }

    private MultiLayerNetwork buildModel() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
            .seed(42)
            .updater(new Adam(0.001))
            .weightInit(WeightInit.XAVIER)
            .list()
            .layer(new LSTM.Builder()
                .nIn(NUM_FEATURES).nOut(64)
                .activation(Activation.TANH)
                .build())
            .layer(new LSTM.Builder()
                .nIn(64).nOut(32)
                .activation(Activation.TANH)
                .build())
            .layer(new RnnOutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
                .nIn(32).nOut(2)
                .activation(Activation.SOFTMAX)
                .build())
            .setInputType(InputType.recurrent(NUM_FEATURES))
            .build();

        MultiLayerNetwork net = new MultiLayerNetwork(conf);
        net.init();
        return net;
    }

    // -------------------------------------------------------------------------
    // Prediction
    // -------------------------------------------------------------------------

    private double predict() {
        if (model == null || history.size() < lookback) return 0.5;

        int start = history.size() - lookback;
        double[] closes  = new double[lookback];
        double[] volumes = new double[lookback];
        for (int t = 0; t < lookback; t++) {
            closes[t]  = history.get(start + t).close().doubleValue();
            volumes[t] = history.get(start + t).volume().doubleValue();
        }

        double minC = min(closes), maxC = max(closes);
        double minV = min(volumes), maxV = max(volumes);
        double rangeC = maxC - minC == 0 ? 1 : maxC - minC;
        double rangeV = maxV - minV == 0 ? 1 : maxV - minV;

        INDArray input = Nd4j.zeros(1, NUM_FEATURES, lookback);
        for (int t = 0; t < lookback; t++) {
            Candle c = history.get(start + t);
            input.putScalar(new int[]{0, 0, t}, (closes[t] - minC) / rangeC);
            input.putScalar(new int[]{0, 1, t}, (volumes[t] - minV) / rangeV);
            input.putScalar(new int[]{0, 2, t}, Math.max(0, Math.min(1,
                computeRsiForCandle(start + t) / 100.0)));
            input.putScalar(new int[]{0, 3, t},
                computeEmaSpreadForCandle(start + t, minC, rangeC));
            double ret = c.open().doubleValue() == 0 ? 0
                : (c.close().doubleValue() - c.open().doubleValue()) / c.open().doubleValue();
            input.putScalar(new int[]{0, 4, t}, Math.max(-0.1, Math.min(0.1, ret)));
        }

        INDArray output = model.rnnTimeStep(input);
        // output shape: [1, 2, lookback] — take last timestep, class 1 = UP
        return output.getDouble(0, 1, lookback - 1);
    }

    // -------------------------------------------------------------------------
    // Online indicator computation from history
    // -------------------------------------------------------------------------

    private void updateIndicators(Candle candle) {
        double close = candle.close().doubleValue();
        // EMA
        double mf = 2.0 / (9 + 1), ms = 2.0 / (21 + 1);
        fastEma = fastEma == 0 ? close : close * mf + fastEma * (1 - mf);
        slowEma = slowEma == 0 ? close : close * ms + slowEma * (1 - ms);
        // RSI
        if (prevClose >= 0) {
            double change = close - prevClose;
            double gain = Math.max(change, 0), loss = Math.max(-change, 0);
            rsiCount++;
            if (rsiCount <= 14) {
                rsiSumGain += gain; rsiSumLoss += loss; rsiSumCount++;
                if (rsiCount == 14) {
                    avgGain = rsiSumGain / 14; avgLoss = rsiSumLoss / 14;
                }
            } else {
                avgGain = avgGain * 13.0 / 14 + gain / 14;
                avgLoss = avgLoss * 13.0 / 14 + loss / 14;
            }
        }
        prevClose = close;
    }

    private double computeRsiForCandle(int idx) {
        if (rsiCount < 14) return 50.0;
        if (avgLoss == 0) return 100.0;
        double rs = avgGain / avgLoss;
        return 100.0 - 100.0 / (1 + rs);
    }

    private double computeEmaSpreadForCandle(int idx, double minC, double rangeC) {
        if (rangeC == 0) return 0;
        return (fastEma - slowEma) / rangeC;
    }

    // -------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------

    private double min(double[] arr) {
        double m = arr[0];
        for (double v : arr) if (v < m) m = v;
        return m;
    }

    private double max(double[] arr) {
        double m = arr[0];
        for (double v : arr) if (v > m) m = v;
        return m;
    }
}
