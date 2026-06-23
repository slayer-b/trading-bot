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

public class OkxMultiHorizonPredictor {

    private static final int HISTORY_LENGTH = 50; // Input sequence length for LSTM

    // --- GLOBAL TRADING CONFIGURATION CONSTANTS ---
    private static final String TARGET_SYMBOL = "XRP-USDT";
    private static final String CANDLE_INTERVAL = "1H"; // Time resolution (e.g., 15m, 1H, 1D)

    // --- MULTI-VALUE PREDICTION CONFIGURATION ---
    // The model will simultaneously train and predict the price for each offset defined below
    private static final int[] PREDICTION_HORIZONS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
//    private static final int[] PREDICTION_HORIZONS = {1, 2, 3, 4, 5};

    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    static void main(String[] args) {
        OkxMultiHorizonPredictor predictor = new OkxMultiHorizonPredictor();
        predictor.runAiPipeline();
    }

    public void runAiPipeline() {
        try {
            // Find the maximum offset needed to ensure safe array boundary conditions
            int maxHorizon = 0;
            for (int h : PREDICTION_HORIZONS) {
                if (h > maxHorizon) maxHorizon = h;
            }

            System.out.printf("[INFO] 1. Gathering data from OKX for %s (Interval: %s)...%n", TARGET_SYMBOL, CANDLE_INTERVAL);
            List<double[]> rawDataset = fetchOkxMarketData(TARGET_SYMBOL, CANDLE_INTERVAL);

            int minimumRequiredCandles = HISTORY_LENGTH + maxHorizon;
            if (rawDataset.size() < minimumRequiredCandles) {
                System.err.printf("[ERROR] Insufficient data. Got %d candles, but need at least %d for maximum horizon of %d.%n",
                        rawDataset.size(), minimumRequiredCandles, maxHorizon);
                return;
            }

            System.out.println("[INFO] 2. Normalizing features and mapping multi-value output array...");
            double maxPrice = rawDataset.stream().mapToDouble(d -> d[0]).max().orElse(1.0);
            double minPrice = rawDataset.stream().mapToDouble(d -> d[0]).min().orElse(0.0);
            double priceRange = (maxPrice - minPrice == 0) ? 1.0 : (maxPrice - minPrice);

            // Shape dimensions: [Batch size, Input features, Time sequence lengths]
            INDArray features = Nd4j.create(new int[]{1, 2, HISTORY_LENGTH});
            // Labels shape output matches the array size of target horizons
            INDArray labels = Nd4j.create(new int[]{1, PREDICTION_HORIZONS.length, HISTORY_LENGTH});

            // Map features to multiple future targets simultaneously
            for (int i = 0; i < HISTORY_LENGTH; i++) {
                double[] currentCandle = rawDataset.get(i);

                features.putScalar(new int[]{0, 0, i}, (currentCandle[0] - minPrice) / priceRange); // Price Feature
                features.putScalar(new int[]{0, 1, i}, currentCandle[1]);                         // Volume Feature

                // Populate label array coordinates for each requested target offset
                for (int hIndex = 0; hIndex < PREDICTION_HORIZONS.length; hIndex++) {
                    int currentHorizonShift = PREDICTION_HORIZONS[hIndex];
                    double[] futureCandle = rawDataset.get(i + currentHorizonShift);

                    labels.putScalar(new int[]{0, hIndex, i}, (futureCandle[0] - minPrice) / priceRange);
                }
            }

            System.out.println("[INFO] 3. Initializing multi-output LSTM neural network architecture...");
            MultiLayerNetwork net = createLstmNetwork();
            net.init();

            System.out.printf("[INFO] 4. Training model over %d target horizons parallelly...%n", PREDICTION_HORIZONS.length);
            for (int epoch = 0; epoch < 60; epoch++) {
                net.fit(features, labels);
            }
            System.out.println("[INFO] Training routine completed successfully.");

            System.out.println("[INFO] 5. Executing forward multidimensional inference...");
            INDArray currentFeatures = Nd4j.create(new int[]{1, 2, HISTORY_LENGTH});
            for (int i = 0; i < HISTORY_LENGTH; i++) {
                double[] data = rawDataset.get(rawDataset.size() - HISTORY_LENGTH + i);
                currentFeatures.putScalar(new int[]{0, 0, i}, (data[0] - minPrice) / priceRange);
                currentFeatures.putScalar(new int[]{0, 1, i}, data[1]);
            }

            INDArray output = net.output(currentFeatures);
            double currentActualPrice = rawDataset.get(rawDataset.size() - 1)[0];

            System.out.println("\n=================================================");
            System.out.printf("🤖 MULTI-VALUE AI PREDICTION ENGINE (%s):%n", TARGET_SYMBOL);
            System.out.printf("Current Spot Reference Price: $%.2f%n", currentActualPrice);
            System.out.println("-------------------------------------------------");

            // Extract each calculated target value from output tensor and translate to USDT
            for (int hIndex = 0; hIndex < PREDICTION_HORIZONS.length; hIndex++) {
                double normalizedPredictedPrice = output.getDouble(new int[]{0, hIndex, HISTORY_LENGTH - 1});
                double predictedPriceInUsdt = (normalizedPredictedPrice * priceRange) + minPrice;

                String direction = (predictedPriceInUsdt > currentActualPrice) ? "UPWARDS 📈" : "DOWNWARDS 📉";
                System.out.printf("Predicted Price (+%d %s): $%.2f | Vector: %s%n",
                        PREDICTION_HORIZONS[hIndex], CANDLE_INTERVAL, predictedPriceInUsdt, direction);
            }
            System.out.println("=================================================");

        } catch (Exception e) {
            System.err.println("[CRITICAL] Pipeline crashed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private MultiLayerNetwork createLstmNetwork() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(42)
                .updater(new Adam(0.005))
                .list()
                .layer(0, new LSTM.Builder()
                        .nIn(2) // Price, Volume
                        .nOut(64) // Increased hidden neurons to capture wider multidimensional variances
                        .activation(Activation.TANH)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .layer(1, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .activation(Activation.IDENTITY)
                        .nIn(64)
                        .nOut(PREDICTION_HORIZONS.length) // Output layers match number of constant array dimensions
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .build();

        return new MultiLayerNetwork(conf);
    }

    private List<double[]> fetchOkxMarketData(String okxSymbol, String interval) throws IOException {
        List<double[]> invertedDataset = new ArrayList<>();

        HttpUrl url = new HttpUrl.Builder()
                .scheme("https")
                .host("okx.com")
                .addPathSegment("api")
                .addPathSegment("v5")
                .addPathSegment("market")
                .addPathSegment("candles")
                .addQueryParameter("instId", okxSymbol)
                .addQueryParameter("bar", interval)
                .addQueryParameter("limit", "300")
                .build();

        Request request = new Request.Builder().url(url).build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode root = objectMapper.readTree(response.body().string());
                JsonNode data = root.get("data");

                if (data != null && data.isArray()) {
                    for (int i = data.size() - 1; i >= 0; i--) {
                        JsonNode candleNode = data.get(i);
                        double closePrice = candleNode.get(4).asDouble();
                        double volume = candleNode.get(5).asDouble();
                        invertedDataset.add(new double[]{closePrice, volume});
                    }
                }
            } else {
                System.err.println("[WARN] HTTP request rejected by OKX server. Status code: " + response.code());
            }
        }

        System.out.printf("[LOG] OKX dataset parsed. Total chronologically aligned candles: %d%n", invertedDataset.size());
        return invertedDataset;
    }
}