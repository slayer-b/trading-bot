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

public class OkxAdvancedLstmPredictor {

    private static final int HISTORY_LENGTH = 40; // Окно анализа истории
    private static final String TARGET_SYMBOL = "XRP-USDT"; // Целевой альткоин
    private static final String LEADER_SYMBOL = "BTC-USDT"; // Поводырь рынка
    private static final String CANDLE_INTERVAL = "1D";

    private static final int[] PREDICTION_HORIZONS = {1, 3, 7}; // Уменьшим массив для точности
    private static final int FEATURES_COUNT = 6; // 6 факторов влияния

    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    static void main(String[] args) {
        OkxAdvancedLstmPredictor predictor = new OkxAdvancedLstmPredictor();
        predictor.runAiPipeline();
    }

    public void runAiPipeline() {
        try {
            int maxHorizon = 0;
            for (int h : PREDICTION_HORIZONS) if (h > maxHorizon) maxHorizon = h;

            System.out.printf("[INFO] 1. Downloading synced data for %s and %s from OKX...%n", TARGET_SYMBOL, LEADER_SYMBOL);
            List<double[]> targetData = fetchOkxMarketData(TARGET_SYMBOL, CANDLE_INTERVAL);
            List<double[]> leaderData = fetchOkxMarketData(LEADER_SYMBOL, CANDLE_INTERVAL);

            int minSize = Math.min(targetData.size(), leaderData.size());
            int minimumRequired = HISTORY_LENGTH + maxHorizon;

            if (minSize < minimumRequired) {
                System.err.println("[ERROR] Not enough candles for training.");
                return;
            }

            System.out.println("[INFO] 2. Calculating Technical Indicators (RSI, Trends) on Java...");
            // Формируем расширенный датасет фич
            List<double[]> featureDataset = new ArrayList<>();
            for (int i = 14; i < minSize; i++) { // Начинаем с 14, чтобы хватило истории для подсчета RSI
                double targetPrice = targetData.get(i)[0];
                double targetVol = targetData.get(i)[1];
                double leaderPrice = leaderData.get(i)[0];

                double targetRsi = calculateRsi(targetData, i, 14);
                double leaderRsi = calculateRsi(leaderData, i, 14);

                // Тренд Биткоина (изменение за 3 дня)
                double leaderTrend = (leaderPrice - leaderData.get(i - 3)[0]) / leaderData.get(i - 3)[0];

                featureDataset.add(new double[]{
                        targetPrice, targetVol, targetRsi,
                        leaderPrice, leaderRsi, leaderTrend
                });
            }

            // Нахождение границ для нормализации (MinMax) по каждой фиче отдельно
            double[][] boundaries = calculateBoundaries(featureDataset);

            INDArray features = Nd4j.create(new int[]{1, FEATURES_COUNT, HISTORY_LENGTH});
            INDArray labels = Nd4j.create(new int[]{1, PREDICTION_HORIZONS.length, HISTORY_LENGTH});

            // Заполнение матриц нормализованными индикаторами
            for (int i = 0; i < HISTORY_LENGTH; i++) {
                double[] currentMetrics = featureDataset.get(i);

                for (int f = 0; f < FEATURES_COUNT; f++) {
                    double normValue = (currentMetrics[f] - boundaries[f][0]) / (boundaries[f][1] - boundaries[f][0] == 0 ? 1 : boundaries[f][1] - boundaries[f][0]);
                    features.putScalar(new int[]{0, f, i}, normValue);
                }

                // Таргет — будущая цена XRP (индекс 0 в массиве фич)
                for (int hIndex = 0; hIndex < PREDICTION_HORIZONS.length; hIndex++) {
                    int shift = PREDICTION_HORIZONS[hIndex];
                    double futureTargetPrice = featureDataset.get(i + shift)[0];
                    double normTarget = (futureTargetPrice - boundaries[0][0]) / (boundaries[0][1] - boundaries[0][0]);
                    labels.putScalar(new int[]{0, hIndex, i}, normTarget);
                }
            }

            System.out.println("[INFO] 3. Initializing Advanced LSTM Layers...");
            MultiLayerNetwork net = createAdvancedNetwork();
            net.init();

            System.out.println("[INFO] 4. Training Model (Increased to 150 Epochs)...");
            for (int epoch = 0; epoch < 150; epoch++) {
                net.fit(features, labels);
            }

            System.out.println("[INFO] 5. Running Forward Inference...");
            INDArray currentFeatures = Nd4j.create(new int[]{1, FEATURES_COUNT, HISTORY_LENGTH});
            for (int i = 0; i < HISTORY_LENGTH; i++) {
                double[] metrics = featureDataset.get(featureDataset.size() - HISTORY_LENGTH + i);
                for (int f = 0; f < FEATURES_COUNT; f++) {
                    double normValue = (metrics[f] - boundaries[f][0]) / (boundaries[f][1] - boundaries[f][0] == 0 ? 1 : boundaries[f][1] - boundaries[f][0]);
                    currentFeatures.putScalar(new int[]{0, f, i}, normValue);
                }
            }

            INDArray output = net.output(currentFeatures);
            double currentActualPrice = targetData.get(minSize - 1)[0];

            System.out.println("\n=================================================");
            System.out.printf("🤖 ADVANCED CORRELATION ENGINE (%s):%n", TARGET_SYMBOL);
            System.out.printf("Current Spot Price: $%.4f%n", currentActualPrice);
            System.out.println("-------------------------------------------------");

            for (int hIndex = 0; hIndex < PREDICTION_HORIZONS.length; hIndex++) {
                double normPred = output.getDouble(new int[]{0, hIndex, HISTORY_LENGTH - 1});
                double predictedPrice = normPred * (boundaries[0][1] - boundaries[0][0]) + boundaries[0][0];

                String direction = (predictedPrice > currentActualPrice) ? "UPWARDS 📈" : "DOWNWARDS 📉";
                System.out.printf("Predicted Price (+%d %s): $%.4f | Vector: %s%n",
                        PREDICTION_HORIZONS[hIndex], CANDLE_INTERVAL, predictedPrice, direction);
            }
            System.out.println("=================================================");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private MultiLayerNetwork createAdvancedNetwork() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(1337)
                .updater(new Adam(0.002)) // Меньший шаг обучения для предотвращения переобучения
                .list()
                .layer(0, new LSTM.Builder()
                        .nIn(FEATURES_COUNT) // 6 индикаторов на входе
                        .nOut(128) // Увеличили емкость памяти сети до 128 нейронов
                        .activation(Activation.TANH)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .layer(1, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .activation(Activation.IDENTITY)
                        .nIn(128)
                        .nOut(PREDICTION_HORIZONS.length)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .build();
        return new MultiLayerNetwork(conf);
    }

    /**
     * Алгоритмический расчет индикатора RSI на чистом Java
     */
    private double calculateRsi(List<double[]> candles, int currentIndex, int period) {
        double gains = 0, losses = 0;
        for (int i = currentIndex - period + 1; i <= currentIndex; i++) {
            double difference = candles.get(i)[0] - candles.get(i - 1)[0];
            if (difference > 0) gains += difference;
            else losses -= difference;
        }
        if (losses == 0) return 100;
        double rs = (gains / period) / (losses / period);
        return 100 - (100 / (1 + rs));
    }

    private double[][] calculateBoundaries(List<double[]> dataset) {
        double[][] bounds = new double[FEATURES_COUNT][2];
        for (int f = 0; f < FEATURES_COUNT; f++) {
            final int field = f;
            bounds[f][0] = dataset.stream().mapToDouble(d -> d[field]).min().orElse(0);
            bounds[f][1] = dataset.stream().mapToDouble(d -> d[field]).max().orElse(1);
        }
        return bounds;
    }

    private List<double[]> fetchOkxMarketData(String symbol, String interval) throws IOException {
        List<double[]> dataset = new ArrayList<>();
        HttpUrl url = new HttpUrl.Builder().scheme("https").host("okx.com")
                .addPathSegment("api").addPathSegment("v5").addPathSegment("market").addPathSegment("candles")
                .addQueryParameter("instId", symbol).addQueryParameter("bar", interval).addQueryParameter("limit", "300").build();

        try (Response response = httpClient.newCall(new Request.Builder().url(url).build()).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode data = objectMapper.readTree(response.body().string()).get("data");
                if (data != null && data.isArray()) {
                    for (int i = data.size() - 1; i >= 0; i--) {
                        dataset.add(new double[]{data.get(i).get(4).asDouble(), data.get(i).get(5).asDouble()});
                    }
                }
            }
        }
        return dataset;
    }
}