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

public class Dl4jCryptoPredictor {

    private static final int HISTORY_LENGTH = 50; // Сколько свечей нужно ИИ для анализа прошлого
    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    static void main(String[] args) {
        Dl4jCryptoPredictor predictor = new Dl4jCryptoPredictor();
        predictor.runAiPipeline("BTCUSDT", "BTC-USDT", "15m");
    }

    public void runAiPipeline(String binanceSymbol, String okxSymbol, String interval) {
        try {
            System.out.println("1. Сбор исторических данных для ИИ...");
            List<double[]> rawDataset = fetchCombinedMarketData(binanceSymbol, okxSymbol, interval);

            if (rawDataset.size() < HISTORY_LENGTH + 1) {
                System.err.println("Недостаточно данных для обучения сети. Нужно минимум 51 свеча.");
                return;
            }

            System.out.println("2. Нормализация данных и подготовка тензоров...");
            // Нам нужно нормализовать данные в диапазон, чтобы LSTM работала корректно
            double maxPrice = rawDataset.stream().mapToDouble(d -> d[0]).max().orElse(1);
            double minPrice = rawDataset.stream().mapToDouble(d -> d[0]).min().orElse(0);

            // Подготовка массивов ND4J для DL4J (Формат: [BatchSize, Features, TimeSeriesLength])
            INDArray features = Nd4j.create(new int[]{1, 4, HISTORY_LENGTH});
            INDArray labels = Nd4j.create(new int[]{1, 1, HISTORY_LENGTH});

            for (int i = 0; i < HISTORY_LENGTH; i++) {
                double[] currentDay = rawDataset.get(i);
                double[] nextDay = rawDataset.get(i + 1); // Целевое значение - цена на следующем шаге

                // Заполняем фичи (нормализованные)
                features.putScalar(new int[]{0, 0, i}, (currentDay[0] - minPrice) / (maxPrice - minPrice)); // Binance Price
                features.putScalar(new int[]{0, 1, i}, currentDay[1]); // Binance Volume
                features.putScalar(new int[]{0, 2, i}, (currentDay[2] - minPrice) / (maxPrice - minPrice)); // OKX Price
                features.putScalar(new int[]{0, 3, i}, currentDay[3]); // OKX Volume

                // Заполняем таргет (то, что предсказываем — цену Binance на следующем шаге)
                labels.putScalar(new int[]{0, 0, i}, (nextDay[0] - minPrice) / (maxPrice - minPrice));
            }

            System.out.println("3. Конфигурация и инициализация нейросети LSTM...");
            MultiLayerNetwork net = createLstmNetwork();
            net.init();

            System.out.println("4. Обучение нейросети (Эпохи)...");
            for (int epoch = 0; epoch < 40; epoch++) {
                net.fit(features, labels);
            }
            System.out.println("Обучение завершено успешно.");

            System.out.println("5. Генерация прогноза цены на следующий интервал...");
            // Берем последний паттерн данных для прогноза будущего
            INDArray currentFeatures = Nd4j.create(new int[]{1, 4, HISTORY_LENGTH});
            for (int i = 0; i < HISTORY_LENGTH; i++) {
                double[] data = rawDataset.get(rawDataset.size() - HISTORY_LENGTH + i);
                currentFeatures.putScalar(new int[]{0, 0, i}, (data[0] - minPrice) / (maxPrice - minPrice));
                currentFeatures.putScalar(new int[]{0, 1, i}, data[1]);
                currentFeatures.putScalar(new int[]{0, 2, i}, (data[2] - minPrice) / (maxPrice - minPrice));
                currentFeatures.putScalar(new int[]{0, 3, i}, data[3]);
            }

            // Прямой проход по сети (Вывод ИИ)
            INDArray output = net.output(currentFeatures);
            double normalizedPredictedPrice = output.getDouble(new int[]{0, 0, HISTORY_LENGTH - 1});
            
            // Денормализация цены обратно в доллары
            double predictedPriceInUsdt = normalizedPredictedPrice * (maxPrice - minPrice) + minPrice;
            double currentActualPrice = rawDataset.get(rawDataset.size() - 1)[0];

            System.out.println("\n===============================================");
            System.out.printf("🤖 РЕЗУЛЬТАТ НЕЙРОСЕТИ (Deeplearning4j LSTM):%n");
            System.out.printf("Текущая спот-цена (Binance): $%.2f%n", currentActualPrice);
            System.out.printf("Прогноз ИИ на следующую свечу (%s): $%.2f%n", interval, predictedPriceInUsdt);
            System.out.printf("Направление: %s%n", (predictedPriceInUsdt > currentActualPrice) ? "UP 📈" : "DOWN 📉");
            System.out.println("===============================================");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Построение архитектуры рекуррентной сети
     */
    private MultiLayerNetwork createLstmNetwork() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(123)
                .updater(new Adam(0.005)) // Оптимизатор весов
                .list()
                .layer(0, new LSTM.Builder()
                        .nIn(4) // 4 фичи на входе (Цены и объемы с двух бирж)
                        .nOut(32) // Количество скрытых LSTM нейронов
                        .activation(Activation.TANH)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .layer(1, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE) // Среднеквадратичная ошибка для регрессии
                        .activation(Activation.IDENTITY)
                        .nIn(32)
                        .nOut(1) // 1 число на выходе (прогноз цены)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .build();

        return new MultiLayerNetwork(conf);
    }

    /**
     * Метод агрегации истории свечей с двух бирж с защитой от ошибок URL
     */
    /**
     * Исправленный метод агрегации истории свечей с автоматическим выравниванием хронологии
     */
    private List<double[]> fetchCombinedMarketData(String binanceSymbol, String okxSymbol, String interval) throws IOException {
        List<double[]> dataset = new ArrayList<>();

        // 1. Запрашиваем 100 свечей с запасом, чтобы точно хватило 51 валидной свечи
        int requestedLimit = 100;

//        HttpUrl binanceUrl = new HttpUrl.Builder().scheme("https").host("binance.com").addPathSegments("api/v3/klines")
//                .addQueryParameter("symbol", binanceSymbol).addQueryParameter("interval", interval)
//                .addQueryParameter("limit", String.valueOf(requestedLimit)).build();

        HttpUrl okxUrl = new HttpUrl.Builder().scheme("https").host("okx.com").addPathSegments("api/v5/market/candles")
                .addQueryParameter("instId", okxSymbol).addQueryParameter("bar", interval)
                .addQueryParameter("limit", String.valueOf(requestedLimit)).build();

        // Запрос к Binance (данные идут от СТАРЫХ к НОВЫМ)
        List<double[]> bData = new ArrayList<>();
//        try (Response response = httpClient.newCall(new Request.Builder().url(binanceUrl).build()).execute()) {
//            if (response.isSuccessful() && response.body() != null) {
//                JsonNode root = objectMapper.readTree(response.body().string());
//                for (JsonNode n : root) {
//                    bData.add(new double[]{n.get(4).asDouble(), n.get(5).asDouble()}); // close, volume
//                }
//            } else {
//                System.err.println("[Ошибка] Binance вернул код: " + response.code());
//            }
//        }

        // Запрос к OKX (данные идут от НОВЫХ к СТАРЫХ)
        List<double[]> oDataRaw = new ArrayList<>();
        try (Response response = httpClient.newCall(new Request.Builder().url(okxUrl).build()).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode root = objectMapper.readTree(response.body().string());
                JsonNode data = root.get("data");
                if (data != null && data.isArray()) {
                    for (JsonNode n : data) {
                        oDataRaw.add(new double[]{n.get(4).asDouble(), n.get(5).asDouble()}); // close, volume
                    }
                }
            } else {
                System.err.println("[Ошибка] OKX вернул код: " + response.code());
            }
        }

        // Разворачиваем данные OKX, чтобы они совпали по хронологии с Binance (от СТАРЫХ к НОВЫМ)
        List<double[]> oData = new ArrayList<>();
        for (int i = oDataRaw.size() - 1; i >= 0; i--) {
            oData.add(oDataRaw.get(i));
        }

        // Диагностическое логирование
        System.out.printf("[Лог] Получено свечей от Binance: %d, от OKX: %d%n", bData.size(), oData.size());

        // Синхронизируем массивы по наименьшему общему количеству
        int minSize = Math.min(bData.size(), oData.size());
//        for (int i = 0; i < minSize; i++) {
//            dataset.add(new double[]{
//                    bData.get(i)[0], // Binance Price
//                    bData.get(i)[1], // Binance Volume
//                    oData.get(i)[0], // OKX Price
//                    oData.get(i)[1]  // OKX Volume
//            });
//        }

        System.out.printf("[Лог] Итоговый синхронизированный датасет: %d свечей.%n", dataset.size());
        return dataset;
    }
}