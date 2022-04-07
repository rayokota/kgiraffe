package io.kgraph.kgiraffe.schema;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import io.vavr.control.Either;
import org.ojai.Value.Type;

public class SchemaContext {
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final ParsedSchema valueSchema;
    private final Mode mode;
    private final boolean key;
    private boolean root;
    private int nameIndex = 0;

    public SchemaContext(String topic,
                         Either<Type, ParsedSchema> keySchema,
                         ParsedSchema valueSchema,
                         Mode mode,
                         boolean key) {
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.mode = mode;
        this.key = key;
        this.root = true;
    }

    public SchemaContext copy(boolean key) {
        return new SchemaContext(topic, keySchema, valueSchema, mode, key);
    }

    public String topic() {
        return topic;
    }

    public Either<Type, ParsedSchema> keySchema() {
        return keySchema;
    }

    public ParsedSchema valueSchema() {
        return valueSchema;
    }

    public boolean isKey() {
        return key;
    }

    public Mode mode() {
        return mode;
    }

    public boolean isWhere() {
        return mode == Mode.QUERY_WHERE;
    }

    public boolean isOrderBy() {
        return mode == Mode.QUERY_ORDER_BY;
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
            case QUERY_WHERE:
                suffix = "_criteria";
                break;
            case QUERY_ORDER_BY:
                suffix = "_order";
                break;
            case MUTATION:
                suffix = "_insert";
                break;
            case SUBSCRIPTION:
                suffix = "_sub";
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
        QUERY_WHERE,
        QUERY_ORDER_BY,
        MUTATION,
        SUBSCRIPTION,
        OUTPUT
    }
}
