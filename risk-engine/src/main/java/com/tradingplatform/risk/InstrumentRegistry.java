package com.tradingplatform.risk;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The set of instruments the platform will accept orders for, plus the reference price used when
 * an order carries no price of its own.
 *
 * <p>The tradable set is fixed at construction: instrument onboarding is a control-plane action, so
 * it must not be a concurrent mutation on the order-ingress path. Reference prices do change while
 * trading, so each one is an independent {@link AtomicLong}. A slightly stale reference price only
 * affects the notional estimate of a market order, which is why a plain atomic write is enough and
 * no lock is taken here.
 */
public final class InstrumentRegistry {
    private final Map<String, AtomicLong> referencePrices;

    public InstrumentRegistry(Collection<Instrument> instruments) {
        Map<String, AtomicLong> prices = new HashMap<>();
        for (Instrument instrument : instruments) {
            if (prices.putIfAbsent(
                            instrument.symbol(), new AtomicLong(instrument.referencePrice()))
                    != null) {
                throw new IllegalArgumentException("duplicate instrument: " + instrument.symbol());
            }
        }
        this.referencePrices = Map.copyOf(prices);
    }

    public boolean isTradable(String symbol) {
        return symbol != null && referencePrices.containsKey(symbol);
    }

    public Set<String> tradableSymbols() {
        return referencePrices.keySet();
    }

    /** Returns 0 when the instrument is unknown or has never had a price. */
    public long referencePrice(String symbol) {
        AtomicLong price = referencePrices.get(symbol);
        return price == null ? 0 : price.get();
    }

    /** Ignores unknown symbols so a stray execution cannot silently onboard an instrument. */
    public void recordTradedPrice(String symbol, long price) {
        if (price <= 0) {
            throw new IllegalArgumentException("traded price must be positive");
        }
        AtomicLong current = referencePrices.get(symbol);
        if (current != null) {
            current.set(price);
        }
    }

    public record Instrument(String symbol, long referencePrice) {
        public Instrument {
            if (symbol == null || symbol.isBlank()) {
                throw new IllegalArgumentException("instrument symbol is required");
            }
            if (referencePrice < 0) {
                throw new IllegalArgumentException("reference price cannot be negative");
            }
        }
    }
}
