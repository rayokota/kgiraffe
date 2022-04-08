package io.kgraph.kgiraffe.schema;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeReference;
import io.vavr.control.Either;
import org.apache.avro.Schema;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.createInputFieldOp;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.orderByEnum;

public class GraphQLPrimitiveSchemaBuilder extends GraphQLAbstractSchemaBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLPrimitiveSchemaBuilder.class);

    @Override
    public GraphQLInputType createInputType(SchemaContext ctx, Either<Type, ParsedSchema> schema) {
        return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType createOutputType(SchemaContext ctx,
                                              Either<Type, ParsedSchema> schema) {
        return Scalars.GraphQLString;
    }

}
