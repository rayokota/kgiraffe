package io.kgraph.kgiraffe.schema;

import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import io.vavr.control.Either;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public abstract class GraphQLSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLSchemaConverter.class);

    protected final Map<String, GraphQLType> typeCache = new HashMap<>();

    public abstract GraphQLInputType createInputType(SchemaContext ctx,
                                                     Either<Type, ParsedSchema> schema);

    public abstract GraphQLOutputType createOutputType(SchemaContext ctx,
                                                       Either<Type, ParsedSchema> schema);
}
