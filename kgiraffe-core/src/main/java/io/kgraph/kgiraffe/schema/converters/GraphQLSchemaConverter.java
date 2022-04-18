package io.kgraph.kgiraffe.schema.converters;

import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLOutputType;
import io.kgraph.kgiraffe.schema.SchemaContext;
import io.vavr.control.Either;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public abstract class GraphQLSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLSchemaConverter.class);

    protected final Set<String> typeCache = new HashSet<>();

    public abstract GraphQLInputType createInputType(SchemaContext ctx,
                                                     Either<Type, ParsedSchema> schema);

    public abstract GraphQLOutputType createOutputType(SchemaContext ctx,
                                                       Either<Type, ParsedSchema> schema);
}
