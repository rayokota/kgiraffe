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
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.kgraph.kgiraffe.schema.PredicateFilter.Criteria;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.OFFSET_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.PARTITION_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.TIMESTAMP_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.TOPIC_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.orderByEnum;

public class GraphQLAvroSchemaBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLAvroSchemaBuilder.class);

    private final Map<String, GraphQLType> typeCache = new HashMap<>();

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
        try {
            if (ctx.isRoot()) {
                GraphQLInputObjectType type = (GraphQLInputObjectType) typeCache.get(name);
                if (type != null) {
                    return type;
                }
            }
            List<Schema.Field> schemaFields = new ArrayList<>(schema.getFields());
            if (ctx.isRoot()) {
                addKafkaFields(schemaFields);
            }
            List<GraphQLInputObjectField> fields = schemaFields.stream()
                .filter(f -> !f.schema().getType().equals(Schema.Type.NULL))
                .map(f -> createInputField(ctx, schema, f))
                .collect(Collectors.toList());
            GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject()
                .name(name)
                .description(schema.getDoc())
                .fields(fields);

            if (ctx.isRoot()) {
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
                // TODO key
            }
            GraphQLInputObjectType type = builder.build();
            if (ctx.isRoot()) {
                typeCache.put(name, type);
            }
            return type;
        } finally {
            ctx.setRoot(false);
        }
    }

    private GraphQLInputObjectField createInputField(SchemaContext ctx,
                                                     Schema schema,
                                                     Schema.Field field) {
        String name = ctx.qualify(schema.getFullName() + "_" + field.name());
        GraphQLInputType fieldType = createInputType(ctx, field.schema());
        if (ctx.isWhere() && !(fieldType instanceof GraphQLInputObjectType)) {
            fieldType = GraphQLInputObjectType.newInputObject()
                .name(name)
                .description("Criteria expression specification of "
                    + field.name() + " attribute in entity " + schema.getFullName())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Logical.OR.symbol())
                    .description("Logical OR criteria expression")
                    .type(new GraphQLList(new GraphQLTypeReference(name)))
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Logical.AND.symbol())
                    .description("Logical AND criteria expression")
                    .type(new GraphQLList(new GraphQLTypeReference(name)))
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.EQ.symbol())
                    .description("Equals criteria")
                    .type(fieldType)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.NEQ.symbol())
                    .description("Not equals criteria")
                    .type(fieldType)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.LTE.symbol())
                    .description("Less than or equals criteria")
                    .type(fieldType)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.GTE.symbol())
                    .description("Greater or equals criteria")
                    .type(fieldType)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.GT.symbol())
                    .description("Greater than criteria")
                    .type(fieldType)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.LT.symbol())
                    .description("Less than criteria")
                    .type(fieldType)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.IN.symbol())
                    .description("In criteria")
                    .type(new GraphQLList(fieldType))
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(Criteria.NIN.symbol())
                    .description("Not in criteria")
                    .type(new GraphQLList(fieldType))
                    .build())
                .build();
        }
        return GraphQLInputObjectField.newInputObjectField()
            .name(field.name())
            .description(field.doc())
            .type(fieldType)
            .build();
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
        String name = ctx.qualify(schema.getFullName());
        try {
            if (ctx.isRoot()) {
                GraphQLObjectType type = (GraphQLObjectType) typeCache.get(name);
                if (type != null) {
                    return type;
                }
            }
            List<Schema.Field> schemaFields = new ArrayList<>(schema.getFields());
            if (ctx.isRoot()) {
                addKafkaFields(schemaFields);
            }
            List<GraphQLFieldDefinition> fields = schemaFields.stream()
                .filter(f -> !f.schema().getType().equals(Schema.Type.NULL))
                .map(f -> createOutputField(ctx, f))
                .collect(Collectors.toList());
            GraphQLObjectType.Builder builder = GraphQLObjectType.newObject()
                .name(name)
                .description(schema.getDoc())
                .fields(fields);
            GraphQLObjectType type = builder.build();
            if (ctx.isRoot()) {
                typeCache.put(name, type);
            }
            return type;
        } finally {
            ctx.setRoot(false);
        }
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
                    // TODO fix union fetch
                    .dataFetcher(new AttributeFetcher(t.getFullName()))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private void addKafkaFields(List<Schema.Field> schemaFields) {
        schemaFields.add(new Schema.Field(
            TOPIC_ATTR_NAME, Schema.create(Schema.Type.STRING), "Kafka topic"));
        schemaFields.add(new Schema.Field(
            PARTITION_ATTR_NAME, Schema.create(Schema.Type.INT), "Kafka partition"));
        schemaFields.add(new Schema.Field(
            OFFSET_ATTR_NAME, Schema.create(Schema.Type.LONG), "Kafka record offset"));
        schemaFields.add(new Schema.Field(
            TIMESTAMP_ATTR_NAME, Schema.create(Schema.Type.LONG), "Kafka record timestamp"));
    }
}
