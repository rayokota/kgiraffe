package io.kgraph.kgiraffe.schema;

import com.google.common.collect.EnumHashBiMap;

import java.util.Set;

public enum Status {
    STAGED("staged"), REGISTERED("registered"), ERRORED("errored");

    private static final EnumHashBiMap<Status, String> lookup =
        EnumHashBiMap.create(Status.class);

    static {
        for (Status type : Status.values()) {
            lookup.put(type, type.symbol());
        }
    }

    private final String symbol;

    Status(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    public static Status get(String symbol) {
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
