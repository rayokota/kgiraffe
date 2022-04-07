package io.kgraph.kgiraffe.schema;

import com.google.common.collect.EnumHashBiMap;

import java.util.Set;

public enum OrderBy {
    ASC("asc"), DESC("desc");

    private static final EnumHashBiMap<OrderBy, String> lookup =
        EnumHashBiMap.create(OrderBy.class);

    static {
        for (OrderBy type : OrderBy.values()) {
            lookup.put(type, type.symbol());
        }
    }

    private final String symbol;

    OrderBy(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    public static OrderBy get(String symbol) {
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
