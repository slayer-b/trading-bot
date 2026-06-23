package com.tradingbot.predict;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OkxGodModePredictor {

    private static final int HISTORY_LENGTH = 50;
    private static final String TARGET_SYMBOL = "XRP-USDT";
    private static final String CANDLE_INTERVAL = "5m";
    private static final int[] HORIZONS = {3, 6};
    private static final int TICK_DURATION_MINUTES = 15;

    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger tickCount = new AtomicInteger(0);
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    static void main(String[] args) {
        System.out.println("[GOD_MODE] Initializing Deep Engine...");
        new OkxGodModePredictor().startLiveTracking();
    }

    public void startLiveTracking() {
        scheduler.scheduleAtFixedRate(
                this::executeSingleTick, 0,
                TICK_DURATION_MINUTES, TimeUnit.MINUTES
        );
    }

    private void executeSingleTick() {
        try {
            int currentTick = tickCount.incrementAndGet();
            System.out.printf("%n[TICK %d/8] Running...%n", currentTick);

            List<double[]> rawData = fetchOkxMarketData();
            if (rawData.size() < HISTORY_LENGTH + 25) {
                System.err.println("[ERR] Data underflow.");
                return;
            }

            double maxP = rawData.stream().mapToDouble(d -> d[0])
                    .max().orElse(1.0);
            double minP = rawData.stream().mapToDouble(d -> d[0])
                    .min().orElse(0.0);
            double range = (maxP - minP == 0) ? 1.0 : (maxP - minP);

            // 5 Features: Norm Price, Volume, RSI, VolSpike, ATR Volatility
            INDArray features = Nd4j.create(new int[]{1, 5, HISTORY_LENGTH});
            INDArray labels = Nd4j.create(new int[]{1, HORIZONS.length, HISTORY_LENGTH});

            buildTensors(rawData, features, labels, minP, range);

            MultiLayerNetwork net = createLstmNetwork();
            net.init();

            for (int e = 0; e < 100; e++) { // Deep training cycle
                net.fit(features, labels);
            }

            INDArray currentFeatures = Nd4j.create(new int[]{1, 5, HISTORY_LENGTH});
            buildCurrentFeatures(rawData, currentFeatures, minP, range);

            INDArray output = net.output(currentFeatures);
            double livePrice = rawData.get(rawData.size() - 1)[0];

            printConsoleOutput(livePrice, output, minP, range);

            if (currentTick >= 8) {
                System.out.println("[STOP] Execution finalized.");
                scheduler.shutdown();
            }

        } catch (Exception e) {
            System.err.println("[CRITICAL] Error: " + e.getMessage());
        }
    }

    private void buildTensors(List<double[]> data, INDArray f,
                              INDArray l, double min, double r) {
        for (int i = 0; i < HISTORY_LENGTH; i++) {
            double[] curr = data.get(i + 20); // Shift for indicator stability
            double rsi = calculateRsi14(data, i + 20);
            double volS = curr[1] / calculateAvgVol(data, i + 20);
            double atr = calculateAtr14(data, i + 20);

            f.putScalar(new int[]{0, 0, i}, (curr[0] - min) / r);
            f.putScalar(new int[]{0, 1, i}, curr[1]);
            f.putScalar(new int[]{0, 2, i}, rsi / 100.0);
            f.putScalar(new int[]{0, 3, i}, volS);
            f.putScalar(new int[]{0, 4, i}, atr);

            for (int h = 0; h < HORIZONS.length; h++) {
                double[] fut = data.get(i + 20 + HORIZONS[h]);
                l.putScalar(new int[]{0, h, i}, (fut[0] - min) / r);
            }
        }
    }

    private void buildCurrentFeatures(List<double[]> data, INDArray f,
                                      double min, double r) {
        int size = data.size();
        for (int i = 0; i < HISTORY_LENGTH; i++) {
            int idx = size - HISTORY_LENGTH + i;
            double[] curr = data.get(idx);
            double rsi = calculateRsi14(data, idx);
            double volS = curr[1] / calculateAvgVol(data, idx);
            double atr = calculateAtr14(data, idx);

            f.putScalar(new int[]{0, 0, i}, (curr[0] - min) / r);
            f.putScalar(new int[]{0, 1, i}, curr[1]);
            f.putScalar(new int[]{0, 2, i}, rsi / 100.0);
            f.putScalar(new int[]{0, 3, i}, volS);
            f.putScalar(new int[]{0, 4, i}, atr);
        }
    }

    private void printConsoleOutput(double livePrice, INDArray output,
                                    double min, double r) {
        System.out.println("---------------------------------");
        System.out.printf("Live Spot Price: $%.5f%n", livePrice);
        System.out.println("---------------------------------");

        for (int h = 0; h < HORIZONS.length; h++) {
            double norm = output.getDouble(new int[]{0, h, HISTORY_LENGTH - 1});
            double pred = (norm * r) + min;
            int mins = HORIZONS[h] * 5;
            String vec = (pred > livePrice) ? "UP 📈" : "DOWN 📉";

            System.out.printf("%d Mins: $%.5f | Vector: %s%n",
                    mins, pred, vec);
        }
        System.out.println("=================================");
    }

    private MultiLayerNetwork createLstmNetwork() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(42).updater(new Adam(0.002)).list()
                .layer(0, new LSTM.Builder().nIn(5).nOut(64) // Layer 1
                        .activation(Activation.TANH)
                        .weightInit(WeightInit.XAVIER).build())
                .layer(1, new LSTM.Builder().nIn(64).nOut(32) // Deep Layer 2
                        .activation(Activation.TANH)
                        .weightInit(WeightInit.XAVIER).build())
                .layer(2, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .activation(Activation.IDENTITY).nIn(32)
                        .nOut(HORIZONS.length)
                        .weightInit(WeightInit.XAVIER).build())
                .build();
        return new MultiLayerNetwork(conf);
    }

    private double calculateRsi14(List<double[]> data, int idx) {
        double g = 0, l = 0;
        for (int i = idx - 13; i <= idx; i++) {
            double diff = data.get(i)[0] - data.get(i - 1)[0];
            if (diff > 0) g += diff; else l -= diff;
        }
        if (l == 0) return 100.0;
        return 100.0 - (100.0 / (1.0 + ((g / 14.0) / (l / 14.0))));
    }

    private double calculateAvgVol(List<double[]> data, int idx) {
        int start = idx - 19;
        double sum = 0;
        for (int i = start; i <= idx; i++) sum += data.get(i)[1];
        return sum / 20.0;
    }

    private double calculateAtr14(List<double[]> data, int idx) {
        double trSum = 0;
        for (int i = idx - 13; i <= idx; i++) {
            double high = data.get(i)[2];
            double low = data.get(i)[3];
            double prevClose = data.get(i - 1)[0];
            double tr = Math.max(high - low, Math.max(
                    Math.abs(high - prevClose), Math.abs(low - prevClose)
            ));
            trSum += tr;
        }
        return trSum / 14.0;
    }

    private List<double[]> fetchOkxMarketData() throws IOException {
        List<double[]> inverted = new ArrayList<>();
        HttpUrl url = new HttpUrl.Builder().scheme("https")
                .host("://okx.com")
                .addPathSegment("api").addPathSegment("v5")
                .addPathSegment("market").addPathSegment("candles")
                .addQueryParameter("instId", TARGET_SYMBOL)
                .addQueryParameter("bar", CANDLE_INTERVAL)
                .addQueryParameter("limit", "180").build();

        Request req = new Request.Builder().url(url).build();
        try (Response resp = httpClient.newCall(req).execute()) {
            if (resp.isSuccessful() && resp.body() != null) {
                JsonNode nodes = mapper.readTree(resp.body().string()).get("data");
                if (nodes != null && nodes.isArray()) {
                    for (int i = nodes.size() - 1; i >= 0; i--) {
                        JsonNode n = nodes.get(i);
                        inverted.add(new double[]{
                                n.get(4).asDouble(), // 0: Close Price
                                n.get(5).asDouble(), // 1: Volume
                                n.get(2).asDouble(), // 2: High Price
                                n.get(3).asDouble()  // 3: Low Price
                        });
                    }
                }
            }
        }
        return inverted;
    }
}