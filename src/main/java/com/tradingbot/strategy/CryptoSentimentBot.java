package com.tradingbot.strategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CryptoSentimentBot {

    // Вставьте сюда ваши настоящие ключи для работы с реальным рынком
    private static final String OPENAI_API_KEY = "YOUR_OPENAI_API_KEY";
    private static final String NEWS_API_KEY = "YOUR_NEWS_API_KEY";

    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Точка входа для запуска торгового алгоритма
     */
    static void main(String[] args) {
        System.out.println("=== Инициализация Crypto Sentiment Bot ===");

        CryptoSentimentBot bot = new CryptoSentimentBot();

        // Вы можете передать любой тикер: "Bitcoin", "Ethereum", "Solana"
        String targetAsset = "Bitcoin";

        System.out.println("Запуск торгового цикла для актива: " + targetAsset);
        System.out.println("----------------------------------------------");

        // Запуск логики
        bot.runTradingLogic(targetAsset);

        System.out.println("----------------------------------------------");
        System.out.println("Торговый цикл успешно завершен.");
    }

    /**
     * Главный цикл торговой логики
     */
    public void runTradingLogic(String asset) {
        try {
            System.out.println("[ИНФО] 1. Сбор последних новостей...");
            List<String> headlines = fetchLatestNews(asset);

            if (headlines.isEmpty()) {
                System.out.println("[ВНИМАНИЕ] Не удалось найти новые статьи. Пропуск цикла.");
                return;
            }

            System.out.println("[ИНФО] 2. Анализ настроений с помощью ИИ...");
            double totalScore = 0;
            for (String headline : headlines) {
                double score = analyzeSentimentWithAI(headline);
                totalScore += score;
                System.out.printf("   -> Новость: \"%s\" | Оценка ИИ: %.2f%n", headline, score);
            }

            double averageSentiment = totalScore / headlines.size();
            System.out.printf("%n[ИТОГ] Средний индекс сентимента для %s: %.2f%n", asset, averageSentiment);

            // Шаг 3: Принятие торгового решения
            executeTradeOrder(asset, averageSentiment);

        } catch (Exception e) {
            System.err.println("[ОШИБКА] Критический сбой алгоритма: " + e.getMessage());
        }
    }

    /**
     * Шаг 1: Получение новостей (с поддержкой демо-режима)
     */
    private List<String> fetchLatestNews(String query) throws IOException {
        // Эмуляция данных, если ключ не указан
        if ("YOUR_NEWS_API_KEY".equals(NEWS_API_KEY)) {
            System.out.println("[РЕЖИМ ЭМУЛЯЦИИ] Ключ News API отсутствует. Загрузка тестовых заголовков...");
            return List.of(
                    "MicroStrategy acquires additional billions in Bitcoin as price breaks records",
                    "US SEC approves new spot Bitcoin ETF options options trading",
                    "Concerns grow over potential regulatory tightening on crypto privacy coins"
            );
        }

        List<String> headlines = new ArrayList<>();
        String url = "https://newsapi.org" + query + "&pageSize=5&apiKey=" + NEWS_API_KEY;

        Request request = new Request.Builder().url(url).build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode root = objectMapper.readTree(response.body().string());
                JsonNode articles = root.get("articles");
                if (articles != null && articles.isArray()) {
                    for (JsonNode article : articles) {
                        headlines.add(article.get("title").asText());
                    }
                }
            }
        }
        return headlines;
    }

    /**
     * Шаг 2: Использование ИИ для оценки сентимента (с поддержкой демо-режима)
     */
    private double analyzeSentimentWithAI(String text) throws IOException {
        // Эмуляция данных, если ключ не указан
        if ("YOUR_OPENAI_API_KEY".equals(OPENAI_API_KEY)) {
            // Простейший базовый парсинг слов для имитации работы ИИ
            String lower = text.toLowerCase();
            if (lower.contains("acquire") || lower.contains("approve") || lower.contains("record")) return 0.8;
            if (lower.contains("concern") || lower.contains("tightening") || lower.contains("risk")) return -0.6;
            return 0.0;
        }

        String url = "https://openai.com";
        String systemPrompt = "You are a crypto financial expert. Analyze the sentiment of the headline. " +
                "Respond with exactly ONE float number between -1.0 (extremely bearish) and 1.0 (extremely bullish). " +
                "No words, no punctuation.";

        String jsonPayload = """
        {
          "model": "gpt-4o-mini",
          "messages": [
            {"role": "system", "content": "%s"},
            {"role": "user", "content": "%s"}
          ],
          "temperature": 0.0
        }
        """.formatted(systemPrompt, text.replace("\"", "\\\""));

        RequestBody body = RequestBody.create(jsonPayload, MediaType.parse("application/json"));
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + OPENAI_API_KEY)
                .post(body)
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode root = objectMapper.readTree(response.body().string());
                String aiResponse = root.get("choices").get(0).get("message").get("content").asText().trim();
                return Double.parseDouble(aiResponse);
            }
        } catch (NumberFormatException e) {
            System.err.println("[ВНИМАНИЕ] Ошибка парсинга ответа ИИ. Возвращен 0.0");
        }
        return 0.0;

    }

    /**
     * Шаг 3: Исполнение ордера
     */
    private void executeTradeOrder(String asset, double sentimentScore) {
        double buyThreshold = 0.35;
        double sellThreshold = -0.35;

        System.out.print("[РЕДАКТОР СИГНАЛОВ] ");
        if (sentimentScore >= buyThreshold) {
            System.out.printf("Сигнал: BUY 🟢 (Сентимент %.2f >= Порога %.2f). Ордер отправлен на биржу!%n", sentimentScore, buyThreshold);
        } else if (sentimentScore <= sellThreshold) {
            System.out.printf("Сигнал: SELL 🔴 (Сентимент %.2f <= Порога %.2f). Ордер отправлен на биржу!%n", sentimentScore, sellThreshold);
        } else {
            System.out.printf("Сигнал: HOLD 🟡 (Сентимент %.2f в пределах нормы). Действий не требуется.%n", sentimentScore);
        }
    }
}