package com.tradingbot.bot;

import com.tradingbot.strategy.BreakoutRetestStrategy;
import com.tradingbot.strategy.TradingStrategy;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Modern Open-Closed compliant factory module.
 * Dynamically resolves state-isolated prototype strategy components directly from the Spring IoC map.
 * Supports overloaded generation signatures to maintain full backward compatibility with the architecture core.
 */
@Component
public class BotFactory {

    private final ApplicationContext appContext;

    /**
     * Dependency injection of the core Spring application context token container.
     */
    public BotFactory(ApplicationContext appContext) {
        this.appContext = appContext;
    }

    /**
     * Legacy compatible fallback signature. Overloaded to prevent any compilation issues
     * in existing Orchestrators or automation test profiles that pass only the strategy name parameter string.
     *
     * @param strategyName The strict strategy identifier matching the Spring @Component name string
     * @return Fully prepared, state-isolated TradingStrategy instance without dynamic market injection
     */
    public TradingStrategy createStrategy(String strategyName) {
        if (strategyName == null) {
            throw new IllegalArgumentException("Strategy resolution query parameter name cannot be null");
        }
        String targetBeanName = strategyName.toLowerCase().trim();
        return appContext.getBean(targetBeanName, TradingStrategy.class);
    }

    /**
     * Production ready signature to build state-isolated prototype strategies and inject the verified market credentials context.
     *
     * @param strategyName The strict strategy identifier matching the Spring @Component name string
     * @param marketConfig The market profile token string from configuration context (e.g., "okx-notrade")
     * @return Fully prepared, state-isolated TradingStrategy instance with dynamic market layout mapping bound
     */
    public TradingStrategy createStrategy(String strategyName, String marketConfig) {
        if (strategyName == null || marketConfig == null) {
            throw new IllegalArgumentException("Strategy resolution query parameters cannot be null");
        }

        String targetBeanName = strategyName.toLowerCase().trim();

        try {
            // 1. Fetch a completely fresh, isolated prototype instance from the Spring container map registry
            TradingStrategy isolatedStrategyInstance = appContext.getBean(targetBeanName, TradingStrategy.class);

            // 2. Safe check: If the retrieved strategy is our Price Action engine, invoke the initialization route
            if (isolatedStrategyInstance instanceof BreakoutRetestStrategy breakoutStrategy) {
                breakoutStrategy.initMarket(marketConfig);
            }

            return isolatedStrategyInstance;

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to dynamically manufacture strategy prototype bean for name: "
                    + targetBeanName + ". Verification details: " + e.getMessage(), e);
        }
    }
}
