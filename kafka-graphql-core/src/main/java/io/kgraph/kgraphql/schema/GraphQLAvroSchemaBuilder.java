package io.kgraph.kgraphql.schema;

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
import io.kgraph.kgraphql.schema.PredicateFilter.Criteria;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kgraph.kgraphql.schema.GraphQLSchemaBuilder.orderByEnum;

public class GraphQLAvroSchemaBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLAvroSchemaBuilder.class);

    public GraphQLInputType createInputType(SchemaContext ctx, Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return createInputRecord(ctx, schema);
            case ENUM:
                return ctx.isOrderBy() ? orderByEnum : createInputEnum(ctx, schema);
            case ARRAY:
                return ctx.isOrderBy() ? orderByEnum : new GraphQLList(createInputType(ctx, schema));
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
            default:
                throw new IllegalArgumentException("Illegal type " + schema.getType());
        }
    }

    private GraphQLInputObjectType createInputRecord(SchemaContext ctx, Schema schema) {
        String name = ctx.qualify(schema.getFullName());
        List<GraphQLInputObjectField> fields = schema.getFields().stream()
            .filter(f -> !f.schema().getType().equals(Schema.Type.NULL))
            .flatMap(f -> createInputField(ctx, schema, f))
            .collect(Collectors.toList());
        GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject()
            .name(ctx.qualify(schema.getFullName()))
            .description(schema.getDoc())
            .fields(fields);

        if (ctx.isRoot()) {
            if (!ctx.isOrderBy()) {
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
            // TODO key
            ctx.setRoot(false);
        }

        return builder.build();
    }

    private Stream<GraphQLInputObjectField> createInputField(SchemaContext ctx,
                                                             Schema schema,
                                                             Schema.Field field) {
        GraphQLInputType fieldType = createInputType(ctx, field.schema());
        if (ctx.isOrderBy() || fieldType instanceof GraphQLInputObjectType) {
            return Stream.of(GraphQLInputObjectField.newInputObjectField()
                .name(field.name())
                .description(field.doc())
                .type(createInputType(ctx, field.schema()))
                .build());
        } else {
            String name = ctx.qualify(schema.getFullName());
            List<GraphQLInputObjectField> fields = new ArrayList<>();
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Logical.OR.symbol())
                .description("Logical OR criteria expression")
                .type(new GraphQLList(new GraphQLTypeReference(name)))
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Logical.AND.symbol())
                .description("Logical AND criteria expression")
                .type(new GraphQLList(new GraphQLTypeReference(name)))
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.EQ.symbol())
                .description("Equals criteria")
                .type(fieldType)
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.NEQ.symbol())
                .description("Not equals criteria")
                .type(fieldType)
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.LTE.symbol())
                .description("Less than or equals criteria")
                .type(fieldType)
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.GTE.symbol())
                .description("Greater or equals criteria")
                .type(fieldType)
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.GT.symbol())
                .description("Greater than criteria")
                .type(fieldType)
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.LT.symbol())
                .description("Less than criteria")
                .type(fieldType)
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.IN.symbol())
                .description("In criteria")
                .type(new GraphQLList(fieldType))
                .build()
            );
            fields.add(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.NIN.symbol())
                .description("Not in criteria")
                .type(new GraphQLList(fieldType))
                .build()
            );
            return fields.stream();
        }
    }

    private GraphQLEnumType createInputEnum(SchemaContext ctx, Schema schema) {
        return GraphQLEnumType.newEnum()
            .name(ctx.qualify(schema.getFullName()))
            .description(schema.getDoc())
            .values(schema.getEnumSymbols().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v)
                    .description(v)
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private GraphQLInputObjectType createInputUnion(SchemaContext ctx, Schema schema) {
        return GraphQLInputObjectType.newInputObject()
            .name(ctx.qualify(schema.getFullName() + "_union_" + ctx.incrementNameIndex()))
            .fields(schema.getTypes().stream()
                .filter(t -> !t.getType().equals(Schema.Type.NULL))
                .map(t -> GraphQLInputObjectField.newInputObjectField()
                    .name(t.getFullName())
                    .type(createInputType(ctx, t))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    public GraphQLOutputType createOutputType(SchemaContext ctx, Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return createOutputRecord(ctx, schema);
            case ENUM:
                return createOutputEnum(ctx, schema);
            case ARRAY:
                return new GraphQLList(createInputType(ctx, schema));
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
            default:
                throw new IllegalArgumentException("Illegal type " + schema.getType());
        }
    }

    private GraphQLObjectType createOutputRecord(SchemaContext ctx, Schema schema) {
        List<GraphQLFieldDefinition> fields = schema.getFields().stream()
            .filter(f -> !f.schema().getType().equals(Schema.Type.NULL))
            .map(f -> GraphQLFieldDefinition.newFieldDefinition()
                .name(f.name())
                .description(f.doc())
                .type(createOutputType(ctx, f.schema()))
                .build())
            .collect(Collectors.toList());
        return GraphQLObjectType.newObject()
            .name(ctx.qualify(schema.getFullName()))
            .description(schema.getDoc())
            .fields(fields)
            .build();
    }

    private GraphQLEnumType createOutputEnum(SchemaContext ctx, Schema schema) {
        return GraphQLEnumType.newEnum()
            .name(ctx.qualify(schema.getFullName()))
            .description(schema.getDoc())
            .values(schema.getEnumSymbols().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v)
                    .description(v)
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private GraphQLObjectType createOutputUnion(SchemaContext ctx, Schema schema) {
        return GraphQLObjectType.newObject()
            .name(ctx.qualify(schema.getFullName() + "_union_" + ctx.incrementNameIndex()))
            .fields(schema.getTypes().stream()
                .filter(t -> !t.getType().equals(Schema.Type.NULL))
                .map(t -> GraphQLFieldDefinition.newFieldDefinition()
                    .name(t.getFullName())
                    .type(createOutputType(ctx, t))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

}
