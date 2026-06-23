# Trading Bot

A reactive Java trading bot built with **Spring Boot + Project Reactor**.

## Architecture

```
application.yml
      │
      ▼
 TradingConfig  ──── creates ────▶  Market (BinanceMarket | NoTradeMarket)
      │                                        │
      │                                        │  tickStream(symbol)  ──▶ Flux<Tick>
      │                                        │
      ├── creates ────▶  TradingStrategy ◀─────┘
      │                  (EMA | RSI | your own)
      │                        │
      │                        │  evaluate(ticks, account)  ──▶ Flux<Signal>
      │                        │
      └── creates ────▶  TradingEngine
                               │
                               ├──▶ AccountService  (balance cache)
                               └──▶ Market.submitOrder()
```

## Key Packages

| Package | Purpose |
|---|---|
| `model` | Immutable value objects: `Tick`, `Order`, `Signal`, `AccountState` |
| `market` | `Market` interface + `BinanceMarket` / `NoTradeMarket` impls |
| `strategy` | `TradingStrategy` interface + `MovingAverageCrossStrategy` / `RsiStrategy` impls |
| `account` | `AccountService` — reactive balance manager |
| `core` | `TradingEngine` — wires everything together |
| `config` | Spring Boot wiring via `application.yml` |

## Configuration (`application.yml`)

```yaml
trading:
  market:   notrade      # notrade | binance
  strategy: ema          # ema | rsi
  symbol:   BTCUSDT
  quantity: 0.001        # base asset per trade

  notrade:
    initialUsdt: 10000   # paper balance

  binance:
    apiKey:    ""
    secretKey: ""
```

## Running

```bash
# Paper trading (safe, no real money)
./mvnw spring-boot:run

# Live Binance trading — add keys to application.yml first!
./mvnw spring-boot:run -Dspring-boot.run.arguments="--trading.market=binance"
```

## Adding a Custom Strategy

Implement `TradingStrategy`:

```java
public class MyStrategy implements TradingStrategy {

    @Override
    public String name() { return "My Strategy"; }

    @Override
    public Flux<Signal> evaluate(Flux<Tick> ticks, Mono<AccountState> account) {
        return ticks
            .filter(tick -> /* your condition */)
            .map(tick -> Signal.builder()
                .action(Signal.Action.BUY)
                .symbol(tick.getSymbol())
                .quantity(BigDecimal.valueOf(0.001))
                .reason("My reason")
                .timestamp(Instant.now())
                .build());
    }
}
```

Then register it in `TradingConfig.strategy()`.

## Adding a Custom Market

Implement `Market` (5 methods):

```java
public class MyMarket implements Market {
    public String name() { ... }
    public Flux<Tick> tickStream(String symbol) { ... }
    public Mono<AccountState> fetchAccountState() { ... }
    public Mono<Order> submitOrder(Order order) { ... }
    public Mono<Order> cancelOrder(String clientOrderId) { ... }
}
```

Then register it in `TradingConfig.market()`.
