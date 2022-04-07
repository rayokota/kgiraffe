package io.kgraph.kgiraffe.schema;

import com.google.common.collect.EnumHashBiMap;

import java.util.Set;

public enum Logical {
    AND("_and"), OR("_or");

    private static final EnumHashBiMap<Logical, String> lookup =
        EnumHashBiMap.create(Logical.class);

    static {
        for (Logical type : Logical.values()) {
            lookup.put(type, type.symbol());
        }
    }

    private final String symbol;

    Logical(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    public static Logical get(String symbol) {
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
