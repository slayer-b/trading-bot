package com.tradingbot.account;

import com.tradingbot.market.Market;
import com.tradingbot.market.impl.NoTradeFuturesMarket;
import com.tradingbot.model.AccountState;
import com.tradingbot.model.BalanceHistory;
import com.tradingbot.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class AccountService {

    private static final Logger log = LoggerFactory.getLogger(AccountService.class);

    private final Market      market;
    private final int         historyCapacity;
    private final String      quoteAsset;

    private final AtomicReference<AccountState>       state      = new AtomicReference<>();
    /** Latest known prices: symbol → last price. Updated by TradingEngine on every tick. */
    private final ConcurrentHashMap<String, BigDecimal> lastPrices = new ConcurrentHashMap<>();

    public AccountService(Market market, int historyCapacity, String quoteAsset) {
        this.market          = market;
        this.historyCapacity = historyCapacity;
        this.quoteAsset      = quoteAsset;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public Mono<AccountState> initialize() {
        return market.fetchAccountState()
            .map(s -> s.toBuilder()
                .balanceHistory(BalanceHistory.withCapacity(historyCapacity))
                .build())
            .doOnNext(s -> {
                state.set(s);
                log.info("[Account] Initialised via {}. Balances: {}, history capacity: {}",
                    market.name(), s.balances(), historyCapacity);
            });
    }

    // -------------------------------------------------------------------------
    // Price feed — called by TradingEngine on every tick
    // -------------------------------------------------------------------------

    /** Keep the latest price and refresh unrealised PnL in the account state. */
    public void updatePrice(String symbol, BigDecimal price) {
        lastPrices.put(symbol, price);
        // Refresh unrealised PnL so the next totalValueUsdt() call is accurate
        if (market instanceof NoTradeFuturesMarket fm) {
            BigDecimal upnl = fm.unrealisedPnl(lastPrices);
            state.updateAndGet(current ->
                current == null ? null : current.withUnrealisedPnl(upnl));
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    public Mono<AccountState> current() {
        AccountState s = state.get();
        return s != null ? Mono.just(s) : initialize();
    }

    public AccountState currentSync() { return state.get(); }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Applies a completed fill, logs total portfolio value, and appends a snapshot.
     */
    public void applyFill(Order order) {
        if (order.status() != Order.Status.FILLED
                && order.status() != Order.Status.PARTIALLY_FILLED) {
            return;
        }

        state.updateAndGet(current -> {
            if (current == null) return null;

            Map<String, BigDecimal> balances = new ConcurrentHashMap<>(current.balances());
            Map<String, Order>      orders   = new ConcurrentHashMap<>(current.openOrders());

            String symbol = order.symbol();
            String quote  = deriveQuote(symbol);
            String base   = deriveBase(symbol, quote);

            BigDecimal qty        = order.filledQuantity();
            BigDecimal price      = order.fillPrice();
            BigDecimal cost       = qty.multiply(price);
            // Commission is deducted from what you receive:
            //   BUY  → you receive base asset, pay cost in quote
            //   SELL → you receive quote asset, pay nothing in base (already sent)
            BigDecimal commission     = market.commissionRate();
            BigDecimal oneMinusComm   = BigDecimal.ONE.subtract(commission);
            BigDecimal qtyAfterComm   = qty.multiply(oneMinusComm);   // for BUY
            BigDecimal costAfterComm  = cost.multiply(oneMinusComm);  // for SELL
            BigDecimal commissionPaid = order.side() == Order.Side.BUY
                ? qty.multiply(commission).multiply(price)   // commission in USDT equiv
                : cost.multiply(commission);

            if (order.side() == Order.Side.BUY) {
                balances.merge(base,  qtyAfterComm,  BigDecimal::add);   // receive base minus fee
                balances.merge(quote, cost.negate(),  BigDecimal::add);   // pay full cost
            } else {
                balances.merge(base,  qty.negate(),   BigDecimal::add);   // send base
                balances.merge(quote, costAfterComm,  BigDecimal::add);   // receive quote minus fee
            }
            orders.remove(order.clientOrderId());

            AccountState updated = AccountState.builder()
                .balances(balances)
                .openOrders(orders)
                .snapshotTime(Instant.now())
                .balanceHistory(current.balanceHistory())
                .build();

            BigDecimal total = updated.totalValueUsdt(lastPrices, quoteAsset);

            log.info("[Account] Fill: {} {} {} @ {} | fee: {} {} | {} {} | {} {} | Total: {} {}",
                order.side(), qty, symbol, price,
                commissionPaid.setScale(6, java.math.RoundingMode.HALF_UP), quote,
                balances.getOrDefault(base,  BigDecimal.ZERO), base,
                balances.getOrDefault(quote, BigDecimal.ZERO), quote,
                total, quoteAsset);

            return updated.recordSnapshot(lastPrices, quoteAsset);
        });
    }

    public void registerOpenOrder(Order order) {
        state.updateAndGet(current -> {
            if (current == null) return null;
            Map<String, Order> orders = new ConcurrentHashMap<>(current.openOrders());
            orders.put(order.clientOrderId(), order);
            return current.withOpenOrders(orders).withSnapshotTime(Instant.now());
        });
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String deriveQuote(String symbol) {
        return symbol.endsWith("USDT") ? "USDT" : symbol.substring(symbol.length() - 3);
    }

    private String deriveBase(String symbol, String quote) {
        return symbol.endsWith(quote) ? symbol.substring(0, symbol.length() - quote.length()) : symbol;
    }
}
