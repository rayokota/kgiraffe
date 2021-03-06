package io.kgraph.kgiraffe.schema.converters;

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
import io.kgraph.kgiraffe.schema.AttributeFetcher;
import io.kgraph.kgiraffe.schema.JavaScalars;
import io.kgraph.kgiraffe.schema.Logical;
import io.kgraph.kgiraffe.schema.SchemaContext;
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

public class GraphQLAvroConverter extends GraphQLSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLAvroConverter.class);

    @Override
    public GraphQLInputType createInputType(SchemaContext ctx, Either<Type, ParsedSchema> schema) {
        return createInputType(ctx, ((AvroSchema) schema.get()).rawSchema());
    }

    private GraphQLInputType createInputType(SchemaContext ctx, Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return createInputRecord(ctx, schema);
            case ENUM:
                return ctx.isOrderBy() ? orderByEnum : createInputEnum(ctx, schema);
            case ARRAY:
                return ctx.isOrderBy()
                    ? orderByEnum
                    : new GraphQLList(createInputType(ctx, schema.getElementType()));
            case MAP:
                return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.Json;
            case UNION:
                return createInputUnion(ctx, schema);
            case FIXED:
            case STRING:
            case BYTES:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLString;
            case INT:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLInt;
            case LONG:
                return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.GraphQLLong;
            case FLOAT:
            case DOUBLE:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLFloat;
            case BOOLEAN:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLBoolean;
            case NULL:
                return ctx.isOrderBy() ? orderByEnum : JavaScalars.GraphQLVoid;
            default:
                throw new IllegalArgumentException("Illegal type " + schema.getType());
        }
    }

    private GraphQLInputType createInputRecord(SchemaContext ctx, Schema schema) {
        String name = ctx.qualify(schema.getFullName());
        GraphQLTypeReference type = ctx.cacheIfAbsent(new AvroSchema(schema), name);
        if (type != null) {
            return type;
        }
        boolean isRoot = ctx.isRoot();
        if (isRoot) {
            ctx.setRoot(false);
        }
        List<GraphQLInputObjectField> fields = schema.getFields().stream()
            .map(f -> createInputField(ctx, schema, f))
            .collect(Collectors.toList());
        GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject()
            .name(name)
            .description(schema.getDoc())
            .fields(fields);

        if (isRoot) {
            if (ctx.isWhere()) {
                builder.field(GraphQLInputObjectField.newInputObjectField()
                        .name(Logical.OR.symbol())
                        .description("Logical operation for expressions")
                        .type(new GraphQLList(new GraphQLTypeReference(name)))
                        .build())
                    .field(GraphQLInputObjectField.newInputObjectField()
                        .name(Logical.AND.symbol())
                        .description("Logical operation for expressions")
                        .type(new GraphQLList(new GraphQLTypeReference(name)))
                        .build());
            }
        }
        return builder.build();
    }

    private GraphQLInputObjectField createInputField(SchemaContext ctx,
                                                     Schema schema,
                                                     Schema.Field field) {
        String name = ctx.qualify(schema.getFullName() + "_" + field.name());
        GraphQLInputType fieldType = createInputType(ctx, field.schema());
        if (ctx.isWhere()
            && !(fieldType instanceof GraphQLInputObjectType)
            && !(fieldType instanceof GraphQLTypeReference)) {
            fieldType = createInputFieldOp(name, fieldType);
        }
        return GraphQLInputObjectField.newInputObjectField()
            .name(field.name())
            .description(field.doc())
            .type(fieldType)
            .build();
    }

    private GraphQLEnumType createInputEnum(SchemaContext ctx, Schema schema) {
        String name = ctx.qualify(schema.getFullName());
        ParsedSchema parsedSchema = new AvroSchema(schema);
        GraphQLEnumType enumType = (GraphQLEnumType) ctx.getCached(parsedSchema);
        if (enumType != null) {
            return enumType;
        }
        enumType = GraphQLEnumType.newEnum()
            .name(name)
            .description(schema.getDoc())
            .values(schema.getEnumSymbols().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v)
                    .value(v)
                    .description(v)
                    .build())
                .collect(Collectors.toList()))
            .build();
        ctx.cache(parsedSchema, enumType);
        return enumType;
    }

    private GraphQLInputObjectType createInputUnion(SchemaContext ctx, Schema schema) {
        return GraphQLInputObjectType.newInputObject()
            .name(ctx.qualify(schema.getFullName() + "_union_" + ctx.incrementNameIndex()))
            .fields(schema.getTypes().stream()
                .map(t -> {
                    GraphQLInputType fieldType = createInputType(ctx, t);
                    if (ctx.isWhere()
                        && !(fieldType instanceof GraphQLInputObjectType)
                        && !(fieldType instanceof GraphQLTypeReference)) {
                        fieldType = createInputFieldOp(
                            ctx, schema.getFullName(), t.getFullName(), fieldType);
                    }
                    return GraphQLInputObjectField.newInputObjectField()
                        .name(t.getFullName())
                        .type(fieldType)
                        .build();
                })
                .collect(Collectors.toList()))
            .build();
    }

    @Override
    public GraphQLOutputType createOutputType(SchemaContext ctx,
                                              Either<Type, ParsedSchema> schema) {
        return createOutputType(ctx, ((AvroSchema) schema.get()).rawSchema());
    }

    public GraphQLOutputType createOutputType(SchemaContext ctx, Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return createOutputRecord(ctx, schema);
            case ENUM:
                return createOutputEnum(ctx, schema);
            case ARRAY:
                return new GraphQLList(createOutputType(ctx, schema.getElementType()));
            case MAP:
                return ExtendedScalars.Json;
            case UNION:
                return createOutputUnion(ctx, schema);
            case FIXED:
            case STRING:
            case BYTES:
                return Scalars.GraphQLString;
            case INT:
                return Scalars.GraphQLInt;
            case LONG:
                return ExtendedScalars.GraphQLLong;
            case FLOAT:
            case DOUBLE:
                return Scalars.GraphQLFloat;
            case BOOLEAN:
                return Scalars.GraphQLBoolean;
            case NULL:
                return JavaScalars.GraphQLVoid;
            default:
                throw new IllegalArgumentException("Illegal type " + schema.getType());
        }
    }

    private GraphQLOutputType createOutputRecord(SchemaContext ctx, Schema schema) {
        String name = ctx.qualify(schema.getFullName());
        GraphQLTypeReference type = ctx.cacheIfAbsent(new AvroSchema(schema), name);
        if (type != null) {
            return type;
        }
        List<GraphQLFieldDefinition> fields = schema.getFields().stream()
            .map(f -> createOutputField(ctx, f))
            .collect(Collectors.toList());
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject()
            .name(name)
            .description(schema.getDoc())
            .fields(fields);
        return builder.build();
    }

    private GraphQLFieldDefinition createOutputField(SchemaContext ctx, Schema.Field field) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(field.name())
            .description(field.doc())
            .type(createOutputType(ctx, field.schema()))
            .dataFetcher(new AttributeFetcher(field.name()))
            .build();
    }

    private GraphQLEnumType createOutputEnum(SchemaContext ctx, Schema schema) {
        String name = ctx.qualify(schema.getFullName());
        ParsedSchema parsedSchema = new AvroSchema(schema);
        GraphQLEnumType enumType = (GraphQLEnumType) ctx.getCached(parsedSchema);
        if (enumType != null) {
            return enumType;
        }
        enumType = GraphQLEnumType.newEnum()
            .name(name)
            .description(schema.getDoc())
            .values(schema.getEnumSymbols().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v)
                    .value(v)
                    .description(v)
                    .build())
                .collect(Collectors.toList()))
            .build();
        ctx.cache(parsedSchema, enumType);
        return enumType;
    }

    private GraphQLObjectType createOutputUnion(SchemaContext ctx, Schema schema) {
        return GraphQLObjectType.newObject()
            .name(ctx.qualify(schema.getFullName() + "_union_" + ctx.incrementNameIndex()))
            .fields(schema.getTypes().stream()
                .map(t -> GraphQLFieldDefinition.newFieldDefinition()
                    .name(t.getFullName())
                    .type(createOutputType(ctx, t))
                    .dataFetcher(new AttributeFetcher(t.getFullName()))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }
}
