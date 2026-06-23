package com.tradingbot.predict;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
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

public class OkxScalpingPredictor {

    private static final int HISTORY_LENGTH = 50;
    private static final String TARGET_SYMBOL = "XRP-USDT";
    private static final String CANDLE_INTERVAL = "5m";
    private static final int[] PREDICTION_HORIZONS = {3, 6};

    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    static void main(String[] args) {
        new OkxScalpingPredictor().runAiPipeline();
    }

    public void runAiPipeline() {
        try {
            int maxHorizon = getParamsMaxHorizon();
            System.out.println("[INFO] Fetching OKX...");
            List<double[]> rawDataset = fetchOkxMarketData();

            int minRequired = HISTORY_LENGTH + maxHorizon;
            if (rawDataset.size() < minRequired) {
                System.err.println("[ERROR] Not enough data.");
                return;
            }

            System.out.println("[INFO] Processing indicators...");
            double maxPrice = rawDataset.stream()
                    .mapToDouble(d -> d[0]).max().orElse(1.0);
            double minPrice = rawDataset.stream()
                    .mapToDouble(d -> d[0]).min().orElse(0.0);
            double priceRange = maxPrice - minPrice;
            if (priceRange == 0) priceRange = 1.0;

            INDArray features = Nd4j.create(
                    new int[]{1, 4, HISTORY_LENGTH});
            INDArray labels = Nd4j.create(
                    new int[]{1, PREDICTION_HORIZONS.length,
                            HISTORY_LENGTH});

            for (int i = 0; i < HISTORY_LENGTH; i++) {
                double[] current = rawDataset.get(i);
                double rsi = calculateRsi14(rawDataset, i);
                double avgVol = calculateAvgVolume(rawDataset, i);
                double volSpike = current[1] / avgVol;

                features.putScalar(new int[]{0, 0, i},
                        (current[0] - minPrice) / priceRange);
                features.putScalar(new int[]{0, 1, i},
                        current[1]);
                features.putScalar(new int[]{0, 2, i},
                        rsi / 100.0);
                features.putScalar(new int[]{0, 3, i},
                        volSpike);

                for (int h = 0; h < PREDICTION_HORIZONS.length; h++) {
                    double[] future = rawDataset.get(
                            i + PREDICTION_HORIZONS[h]);
                    labels.putScalar(new int[]{0, h, i},
                            (future[0] - minPrice) / priceRange);
                }
            }

            System.out.println("[INFO] Building Stacked LSTM...");
            MultiLayerNetwork net = createLstmNetwork();
            net.init();

            System.out.println("[INFO] Training ML model...");
            for (int epoch = 0; epoch < 80; epoch++) {
                net.fit(features, labels);
            }

            System.out.println("[INFO] Running Backtest...");
            runBacktest(net, rawDataset, minPrice, priceRange);

            System.out.println("[INFO] Live Inference...");
            INDArray currentFeatures = Nd4j.create(
                    new int[]{1, 4, HISTORY_LENGTH});
            int dSize = rawDataset.size();

            for (int i = 0; i < HISTORY_LENGTH; i++) {
                int idx = dSize - HISTORY_LENGTH + i;
                double[] data = rawDataset.get(idx);
                double rsi = calculateRsi14(rawDataset, idx);
                double avgVol = calculateAvgVolume(rawDataset, idx);
                double volSpike = data[1] / avgVol;

                currentFeatures.putScalar(new int[]{0, 0, i},
                        (data[0] - minPrice) / priceRange);
                currentFeatures.putScalar(new int[]{0, 1, i},
                        data[1]);
                currentFeatures.putScalar(new int[]{0, 2, i},
                        rsi / 100.0);
                currentFeatures.putScalar(new int[]{0, 3, i},
                        volSpike);
            }

            INDArray output = net.output(currentFeatures);
            double currentActualPrice = rawDataset.get(dSize - 1)[0];

            System.out.println("\n=================================");
            System.out.printf("🤖 SCALPING ENGINE: %s%n",
                    TARGET_SYMBOL);
            System.out.printf("Live Spot Price: $%.5f%n",
                    currentActualPrice);
            System.out.println("---------------------------------");

            for (int h = 0; h < PREDICTION_HORIZONS.length; h++) {
                double normPred = output.getDouble(
                        new int[]{0, h, HISTORY_LENGTH - 1});
                double predictedPrice = (normPred * priceRange)
                        + minPrice;
                int minutes = PREDICTION_HORIZONS[h] * 5;

                String vector = (predictedPrice > currentActualPrice)
                        ? "UP 📈" : "DOWN 📉";
                System.out.printf("%d Mins: $%.5f | Vector: %s%n",
                        minutes, predictedPrice, vector);
            }
            System.out.println("=================================");

        } catch (Exception e) {
            System.err.println("[CRITICAL] Error: " + e.getMessage());
        }
    }

    private void runBacktest(MultiLayerNetwork net,
                             List<double[]> rawDataset,
                             double minPrice,
                             double priceRange) {
        int dSize = rawDataset.size();
        int maxHorizon = getParamsMaxHorizon();
        int testEndIndex = dSize - maxHorizon - 1;
        int testStartIndex = HISTORY_LENGTH;

        if (testStartIndex >= testEndIndex) {
            System.out.println("[WARN] Short dataset for backtest.");
            return;
        }

        int totalTests = 0;
        int[] correctDirections = new int[PREDICTION_HORIZONS.length];

        for (int i = testStartIndex; i <= testEndIndex; i += 5) {
            INDArray testFeatures = Nd4j.create(
                    new int[]{1, 4, HISTORY_LENGTH});

            for (int j = 0; j < HISTORY_LENGTH; j++) {
                int dataIdx = i - HISTORY_LENGTH + j;
                double[] data = rawDataset.get(dataIdx);
                double rsi = calculateRsi14(rawDataset, dataIdx);
                double avgVol = calculateAvgVolume(rawDataset, dataIdx);
                double volSpike = data[1] / avgVol;

                testFeatures.putScalar(new int[]{0, 0, j},
                        (data[0] - minPrice) / priceRange);
                testFeatures.putScalar(new int[]{0, 1, j},
                        data[1]);
                testFeatures.putScalar(new int[]{0, 2, j},
                        rsi / 100.0);
                testFeatures.putScalar(new int[]{0, 3, j},
                        volSpike);
            }

            INDArray output = net.output(testFeatures);
            double basePrice = rawDataset.get(i)[0];
            totalTests++;

            for (int h = 0; h < PREDICTION_HORIZONS.length; h++) {
                double normPred = output.getDouble(
                        new int[]{0, h, HISTORY_LENGTH - 1});
                double predPrice = (normPred * priceRange) + minPrice;
                double actualFuturePrice = rawDataset.get(
                        i + PREDICTION_HORIZONS[h])[0];

                boolean predUp = predPrice > basePrice;
                boolean actualUp = actualFuturePrice > basePrice;

                if (predUp == actualUp) {
                    correctDirections[h]++;
                }
            }
        }

        System.out.println("\n--- BACKTEST ACCURACY REPORT ---");
        for (int h = 0; h < PREDICTION_HORIZONS.length; h++) {
            double accuracy = ((double) correctDirections[h] /
                    totalTests) * 100.0;
            int mins = PREDICTION_HORIZONS[h] * 5;
            System.out.printf("Horizon %d Mins Accuracy: %.2f%%%n",
                    mins, accuracy);
        }
        System.out.println("--------------------------------");
    }

    private MultiLayerNetwork createLstmNetwork() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(42)
                .updater(new Adam(0.002))
                .list()
                .layer(0, new LSTM.Builder().nIn(4).nOut(64)
                        .activation(Activation.TANH)
                        .weightInit(WeightInit.XAVIER).build())
                .layer(1, new LSTM.Builder().nIn(64).nOut(32)
                        .activation(Activation.TANH)
                        .weightInit(WeightInit.XAVIER).build())
                .layer(2, new RnnOutputLayer.Builder(
                        LossFunctions.LossFunction.MSE)
                        .activation(Activation.IDENTITY).nIn(32)
                        .nOut(PREDICTION_HORIZONS.length)
                        .weightInit(WeightInit.XAVIER).build())
                .build();
        return new MultiLayerNetwork(conf);
    }

    private double calculateRsi14(List<double[]> data, int index) {
        if (index < 14) return 50.0;
        double gains = 0, losses = 0;
        for (int i = index - 13; i <= index; i++) {
            double diff = data.get(i)[0] - data.get(i - 1)[0];
            if (diff > 0) gains += diff;
            else losses -= diff;
        }
        if (losses == 0) return 100.0;
        double rs = (gains / 14.0) / (losses / 14.0);
        return 100.0 - (100.0 / (1.0 + rs));
    }

    private double calculateAvgVolume(List<double[]> data, int index) {
        int start = Math.max(0, index - 20);
        double sum = 0;
        for (int i = start; i <= index; i++) sum += data.get(i)[1];
        int count = index - start + 1;
        return count == 0 ? 1.0 : sum / count;
    }

    private int getParamsMaxHorizon() {
        int max = 0;
        for (int h : PREDICTION_HORIZONS) if (h > max) max = h;
        return max;
    }

    private List<double[]> fetchOkxMarketData() throws IOException {
        List<double[]> inverted = new ArrayList<>();// Host has NO prefixes or forward slashes at the start
        HttpUrl url = new HttpUrl.Builder().scheme("https").host("www.okx.com").addPathSegment("api").addPathSegment("v5").addPathSegment("market").addPathSegment("candles").addQueryParameter("instId", TARGET_SYMBOL).addQueryParameter("bar", CANDLE_INTERVAL).addQueryParameter("limit", "200").build();
        Request request = new Request.Builder().url(url).build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode data = objectMapper.readTree(response.body().string()).get("data");
                if (data != null && data.isArray()) {
                    for (int i = data.size() - 1; i >= 0; i--) {
                        JsonNode c = data.get(i);
                        inverted.add(new double[]{c.get(4).asDouble(), c.get(5).asDouble()});
                    }
                }
            }
        }
        return inverted;
    }
}