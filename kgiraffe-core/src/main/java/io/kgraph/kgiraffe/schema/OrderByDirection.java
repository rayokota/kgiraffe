package io.kgraph.kgiraffe.schema;

import com.google.common.collect.EnumHashBiMap;

import java.util.Set;

public enum OrderByDirection {
    ASC("asc"), DESC("desc");

    private static final EnumHashBiMap<OrderByDirection, String> lookup =
        EnumHashBiMap.create(OrderByDirection.class);

    static {
        for (OrderByDirection type : OrderByDirection.values()) {
            lookup.put(type, type.symbol());
        }
    }

    private final String symbol;

    OrderByDirection(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    public static OrderByDirection get(String symbol) {
        return lookup.inverse().get(symbol);
    }

    public static Set<String> symbols() {
        return lookup.inverse().keySet();
    }

    @Override
    public String toString() {
        return symbol();
    }
}
