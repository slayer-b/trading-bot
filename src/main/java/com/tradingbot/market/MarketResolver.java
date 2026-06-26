package com.tradingbot.market;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingbot.market.config.MarketUrls;
import com.tradingbot.market.impl.NoTradeMarket;
import com.tradingbot.market.impl.OkxMarket;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Highly scalable infrastructure resolver component responsible for manufacturing
 * concrete exchange market environments cleanly without field-level hardcoding.
 */
@Component
public class MarketResolver {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final ApplicationContext appContext;

    // Thread-safe map cache to ensure singleton style re-usability per unique market string configuration
    private final Map<String, Market> marketCache = new ConcurrentHashMap<>();

    /**
     * Spring constructor injection for required framework resources.
     */
    public MarketResolver(WebClient webClient, ObjectMapper objectMapper, ApplicationContext appContext) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
        this.appContext = appContext;
    }

    /**
     * Resolves, instantiates with respective credentials, and caches the appropriate Market environment.
     *
     * @param marketConfigString Raw string value from configuration (e.g., "okx", "okx-notrade", "binance")
     * @return Fully prepared and cached Market instance
     */
    public Market resolveMarket(String marketConfigString) {
        if (marketConfigString == null || marketConfigString.isBlank()) {
            throw new IllegalArgumentException("Market configuration token string cannot be null or empty");
        }

        String normalizedKey = marketConfigString.toLowerCase().trim();

        return marketCache.computeIfAbsent(normalizedKey, configKey -> {
            System.out.println("[MARKET-RESOLVER] Cache miss. Manufacturing secure environment context for: " + configKey);

            Market baseExchangeMarket = null;

            // Branching via decoupled string checks
            if (configKey.contains("okx")) {
                // Safely extract market environment specific configuration properties via Spring Environment or custom context beans
                org.springframework.core.env.Environment env = appContext.getEnvironment();

                String apiKey = env.getProperty("trading.okx.api-key", "");
                String secretKey = env.getProperty("trading.okx.secret-key", "");
                String passphrase = env.getProperty("trading.okx.passphrase", "");

                String commissionStr = env.getProperty("trading.okx.commission-rate", "0.001");
                BigDecimal commissionRate = new BigDecimal(commissionStr);

                // Fetching MarketUrls bean directly from the active context
                MarketUrls marketUrls = appContext.getBean(MarketUrls.class);

                // Instantiate OkxMarket directly with its dynamic parameters resolved on demand
                baseExchangeMarket = new OkxMarket(
                        this.webClient,
                        this.objectMapper,
                        marketUrls,
                        apiKey,
                        secretKey,
                        passphrase,
                        commissionRate
                );

            } else if (configKey.contains("binance")) {
                // Future expansion block: Binance parameters would be fetched dynamically from 'trading.binance.*' keys
                // baseExchangeMarket = new BinanceMarket(this.webClient, this.objectMapper, ...);
            }

            if (baseExchangeMarket == null) {
                throw new IllegalStateException("Failed to resolve base exchange market implementation for configuration: " + configKey);
            }

            // Wrap into virtual sandbox layer if it is a paper simulation profile execution trace
            if (configKey.contains("notrade") || configKey.contains("paper")) {
                System.out.println("[MARKET-RESOLVER] Encapsulating market within virtual paper trading NoTradeMarket layer.");
                return new NoTradeMarket(baseExchangeMarket);
            }

            return baseExchangeMarket;
        });
    }
}
