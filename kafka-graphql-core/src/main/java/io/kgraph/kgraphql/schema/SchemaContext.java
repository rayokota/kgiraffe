package io.kgraph.kgraphql.schema;

import java.util.Objects;

public class SchemaContext {
    private final String topic;
    private final Mode mode;
    private boolean root;
    private int nameIndex = 0;

    public SchemaContext(String topic, Mode mode) {
        this.topic = topic;
        this.mode = mode;
        this.root = true;
    }

    public String topic() {
        return topic;
    }

    public Mode mode() {
        return mode;
    }

    public boolean isOrderBy() {
        return mode == Mode.ORDER_BY;
    }

    public boolean isRoot() {
        return root;
    }

    public void setRoot(boolean root) {
        this.root = root;
    }

    public String qualify(String name) {
        String suffix;
        switch (mode) {
            case INPUT:
                suffix = "_criteria";
                break;
            case ORDER_BY:
                suffix = "_order";
                break;
            case OUTPUT:
            default:
                suffix = "";
                break;
        }
        return topic + "_" + name.replace('.', '_') + suffix;
    }

    public int incrementNameIndex() {
        return ++nameIndex;
    }

    public enum Mode {
        INPUT,
        ORDER_BY,
        OUTPUT
    }
}
