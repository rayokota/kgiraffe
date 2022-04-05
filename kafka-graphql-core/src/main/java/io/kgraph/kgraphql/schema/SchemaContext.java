package io.kgraph.kgraphql.schema;

import java.util.Objects;

public class SchemaContext {
    private final String topic;
    private final Mode mode;

    public SchemaContext(String topic, Mode mode) {
        this.topic = topic;
        this.mode = mode;
    }

    public String topic() {
        return topic;
    }

    public Mode mode() {
        return mode;
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

    @Override
    public int hashCode() {
        return Objects.hash(topic, mode);
    }

    @Override
    public String toString() {
        return "SchemaContext{"
            + "topic='" + topic + '\''
            + ", mode='" + mode + '\''
            + '}';
    }

    public enum Mode {
        INPUT,
        ORDER_BY,
        OUTPUT
    }
}
