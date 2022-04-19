package io.kgraph.kgiraffe.schema;

import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.vavr.Tuple2;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public class SchemaContext {
    private final String name;
    private final Mode mode;
    private boolean root;
    private int nameIndex = 0;
    private final Map<Tuple2<Mode, ParsedSchema>, GraphQLType> schemaCache;

    public SchemaContext(String name,
                         Mode mode) {
        this.name = name;
        this.mode = mode;
        this.root = true;
        this.schemaCache = new HashMap<>();
    }

    public String name() {
        return name;
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

    public String qualify(String name1, String name2) {
        return qualify(name1 != null ? name1 + "_" + name2 : name2);
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
        return this.name + "_" + name.replace('.', '_') + suffix;
    }

    public int incrementNameIndex() {
        return ++nameIndex;
    }

    public GraphQLTypeReference cacheIfAbsent(ParsedSchema schema, String name) {
        Tuple2<Mode, ParsedSchema> key = new Tuple2<>(mode, schema);
        return (GraphQLTypeReference) schemaCache.putIfAbsent(key, new GraphQLTypeReference(name));
    }

    public GraphQLType getCached(ParsedSchema schema) {
        Tuple2<Mode, ParsedSchema> key = new Tuple2<>(mode, schema);
        return schemaCache.get(key);
    }

    public GraphQLType cache(ParsedSchema schema, GraphQLType type) {
        Tuple2<Mode, ParsedSchema> key = new Tuple2<>(mode, schema);
        return schemaCache.put(key, type);
    }

    public enum Mode {
        QUERY_WHERE,
        QUERY_ORDER_BY,
        MUTATION,
        SUBSCRIPTION,
        OUTPUT
    }
}
