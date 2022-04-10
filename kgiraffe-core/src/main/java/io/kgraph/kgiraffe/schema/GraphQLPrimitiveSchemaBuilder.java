package io.kgraph.kgiraffe.schema;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLOutputType;
import io.vavr.control.Either;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.orderByEnum;

public class GraphQLPrimitiveSchemaBuilder extends GraphQLAbstractSchemaBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLPrimitiveSchemaBuilder.class);

    @Override
    public GraphQLInputType createInputType(SchemaContext ctx, Either<Type, ParsedSchema> schema) {

        switch (schema.getLeft()) {
            case STRING:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLString;
            case SHORT:
            case INT:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLInt;
            case LONG:
                return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.GraphQLLong;
            case FLOAT:
            case DOUBLE:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLFloat;
            case BINARY:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLString;
            default:
                throw new IllegalArgumentException("Illegal type " + schema.getLeft());
        }
    }

    @Override
    public GraphQLOutputType createOutputType(SchemaContext ctx,
                                              Either<Type, ParsedSchema> schema) {
        switch (schema.getLeft()) {
            case STRING:
                return Scalars.GraphQLString;
            case SHORT:
            case INT:
                return Scalars.GraphQLInt;
            case LONG:
                return ExtendedScalars.GraphQLLong;
            case FLOAT:
            case DOUBLE:
                return Scalars.GraphQLFloat;
            case BINARY:
                return Scalars.GraphQLString;
            default:
                throw new IllegalArgumentException("Illegal type " + schema.getLeft());
        }
    }
}
