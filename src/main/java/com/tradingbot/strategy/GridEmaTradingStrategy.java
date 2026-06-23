package com.tradingbot.strategy;

import com.tradingbot.model.AccountState;
import com.tradingbot.model.Candle;
import com.tradingbot.model.ReactiveGridLevel;
import com.tradingbot.model.Signal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class GridEmaTradingStrategy implements TradingStrategy {

    private final BigDecimal gridSpacingPercent;
    private final BigDecimal orderSizeUSD;
    private final int totalGridLines;
    private final int emaPeriod;
    private final String quoteAsset; // Например, "USDT"

    // Состояние сетки в памяти
    private final List<ReactiveGridLevel> gridLevels = new ArrayList<>();
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private BigDecimal lastEmaValue = BigDecimal.ZERO;

    public GridEmaTradingStrategy(double gridSpacingPercent, double orderSizeUSD, int totalGridLines, int emaPeriod, String quoteAsset) {
        this.gridSpacingPercent = BigDecimal.valueOf(gridSpacingPercent).divide(BigDecimal.valueOf(100.0), 4, RoundingMode.HALF_UP);
        this.orderSizeUSD = BigDecimal.valueOf(orderSizeUSD);
        this.totalGridLines = totalGridLines;
        this.emaPeriod = emaPeriod;
        this.quoteAsset = quoteAsset;
    }

    @Override
    public String name() {
        return "Grid-Ema-Trend";
    }

    @Override
    public Flux<Signal> evaluate(Flux<Candle> candles, Mono<AccountState> accountState, String symbol) {
        // Комбинируем поток свечей с текущим состоянием аккаунта для проверки баланса
        return candles.flatMapSequential(candle -> 
            accountState.flatMapMany(account -> {
                
                BigDecimal closePrice = candle.close();
                BigDecimal highPrice = candle.high();
                BigDecimal lowPrice = candle.low();

                // 1. Инициализация сетки при первой свече
                if (!isInitialized.get()) {
                    initializeGrid(closePrice);
                    isInitialized.set(true);
                    return Flux.empty();
                }

                // 2. Рассчитываем динамический тренд по EMA
                updateEma(closePrice);

                List<Signal> generatedSignals = new ArrayList<>();
                String baseAsset = parseBaseAsset(symbol, quoteAsset);

                // 3. Проверяем пересечение уровней сетки экстремумами завершенной свечи
                for (ReactiveGridLevel level : gridLevels) {
                    if (!level.isActive()) continue;

                    // Условие BUY: цена падала до Low, уровень задет, тренд бычий (Close > EMA)
                    if (level.isBuyOrder() 
                            && lowPrice.compareTo(level.getPrice()) <= 0 
                            && closePrice.compareTo(lastEmaValue) > 0) {
                        
                        // Защитный волл: проверяем, хватает ли USDT на покупку
                        BigDecimal availableUsdt = account.balance(quoteAsset);
                        if (availableUsdt.compareTo(orderSizeUSD) >= 0) {
                            
                            generatedSignals.add(createSignal(Signal.Action.BUY, symbol, level.getPrice(), "Grid buy trigger above EMA"));
                            level.setActive(false);
                            flipGridLevel(level, false); // Переворот уровня в SELL
                        }
                    } 
                    // Условие SELL: цена росла до High, уровень задет, тренд медвежий (Close < EMA)
                    else if (!level.isBuyOrder() 
                            && highPrice.compareTo(level.getPrice()) >= 0 
                            && closePrice.compareTo(lastEmaValue) < 0) {
                        
                        // Защитный волл: проверяем, есть ли на балансе базовый актив для продажи
                        BigDecimal assetQuantity = orderSizeUSD.divide(level.getPrice(), 8, RoundingMode.HALF_UP);
                        BigDecimal availableAsset = account.balance(baseAsset);
                        
                        if (availableAsset.compareTo(assetQuantity) >= 0) {
                            
                            generatedSignals.add(createSignal(Signal.Action.SELL, symbol, level.getPrice(), "Grid sell trigger below EMA"));
                            level.setActive(false);
                            flipGridLevel(level, true); // Переворот уровня в BUY
                        }
                    }
                }

                return Flux.fromIterable(generatedSignals);
            })
        );
    }

    private void initializeGrid(BigDecimal currentPrice) {
        this.lastEmaValue = currentPrice;
        gridLevels.clear();

        BigDecimal halfGrid = BigDecimal.valueOf(totalGridLines / 2.0);
        BigDecimal startPrice = currentPrice.subtract(halfGrid.multiply(currentPrice).multiply(gridSpacingPercent));

        for (int i = 0; i < totalGridLines; i++) {
            BigDecimal levelOffset = BigDecimal.valueOf(i).multiply(currentPrice).multiply(gridSpacingPercent);
            BigDecimal levelPrice = startPrice.add(levelOffset);
            boolean isBuyOrder = levelPrice.compareTo(currentPrice) < 0;
            gridLevels.add(new ReactiveGridLevel(levelPrice, isBuyOrder));
        }
    }

    private void updateEma(BigDecimal closePrice) {
        BigDecimal multiplier = BigDecimal.valueOf(2.0 / (emaPeriod + 1));
        BigDecimal priceDiff = closePrice.subtract(lastEmaValue);
        this.lastEmaValue = priceDiff.multiply(multiplier).add(lastEmaValue);
    }

    private void flipGridLevel(ReactiveGridLevel level, boolean nextIsBuy) {
        BigDecimal priceShift = level.getPrice().multiply(gridSpacingPercent);
        BigDecimal newPrice = nextIsBuy ? level.getPrice().subtract(priceShift) : level.getPrice().add(priceShift);
        level.updateLevel(newPrice, nextIsBuy);
    }

    private Signal createSignal(Signal.Action action, String symbol, BigDecimal limitPrice, String reason) {
        return Signal.builder()
                .action(action)
                .symbol(symbol)
                .usdtAmount(orderSizeUSD)
                .limitPrice(limitPrice)
                .reason(reason)
                .timestamp(Instant.now())
                .build();
    }

    private String parseBaseAsset(String symbol, String quoteAsset) {
        if (symbol.endsWith(quoteAsset)) {
            return symbol.substring(0, symbol.length() - quoteAsset.length());
        }
        return symbol; // Фолбэк, если структура торговой пары нестандартная
    }
}
