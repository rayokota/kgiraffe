package io.kgraph.kgiraffe.schema;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.schema.PredicateFilter.Criteria;
import io.kgraph.kgiraffe.schema.SchemaContext.Mode;
import io.vavr.control.Either;
import org.apache.avro.Schema;
import org.everit.json.schema.ObjectSchema;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

/**
 * A wrapper for the {@link graphql.schema.GraphQLSchema.Builder}.
 */
public class GraphQLSchemaBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(GraphQLSchemaBuilder.class);

    public static final String QUERY_ROOT = "query_root";
    public static final String MUTATION_ROOT = "mutation_root";
    public static final String SUBSCRIPTION_ROOT = "subscription_root";

    public static final String LIMIT_PARAM_NAME = "limit";
    public static final String OFFSET_PARAM_NAME = "offset";
    public static final String ORDER_BY_PARAM_NAME = "order_by";
    public static final String WHERE_PARAM_NAME = "where";

    public static final String INSERT_PARAM_NAME = "insert";

    // TODO
    public static final String KEY_ATTR_NAME = "key";
    // TODO for Protobuf
    public static final String KEY_TYPE_ATTR_NAME = "key_type";
    // TODO remove _value
    public static final String VALUE_ATTR_NAME = "value";
    // TODO for Protobuf
    public static final String VALUE_TYPE_ATTR_NAME = "value_type";
    public static final String HEADERS_ATTR_NAME = "headers";
    public static final String TOPIC_ATTR_NAME = "topic";
    public static final String PARTITION_ATTR_NAME = "partition";
    public static final String OFFSET_ATTR_NAME = "offset";
    public static final String TIMESTAMP_ATTR_NAME = "timestamp";

    private final KGiraffeEngine engine;
    private final List<String> topics;
    private final GraphQLAvroSchemaBuilder avroBuilder;

    private final Map<String, GraphQLType> typeCache = new HashMap<>();

    public static final GraphQLEnumType orderByEnum =
        GraphQLEnumType.newEnum()
            .name("order_by_enum")
            .description("Specifies the direction (ascending/descending) for sorting a field")
            .value(OrderBy.ASC.symbol(), OrderBy.ASC.symbol(), "Ascending")
            .value(OrderBy.DESC.symbol(), OrderBy.DESC.symbol(), "Descending")
            .build();

    public GraphQLSchemaBuilder(KGiraffeEngine engine,
                                List<String> topics) {
        this.engine = engine;
        this.topics = topics;
        this.avroBuilder = new GraphQLAvroSchemaBuilder();
    }

    /**
     * @return A freshly built {@link graphql.schema.GraphQLSchema}
     */
    public GraphQLSchema getGraphQLSchema() {
        GraphQLSchema.Builder schema = GraphQLSchema.newSchema()
            .query(getQueryType())
            .mutation(getMutationType())
            .subscription(getSubscriptionType());
        return schema.build();
    }

    private GraphQLObjectType getQueryType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder queryType = GraphQLObjectType.newObject()
            .name(QUERY_ROOT)
            .description("Queries for Kafka topics");
        queryType.fields(topics.stream()
            .flatMap(t -> getQueryFieldDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return queryType.build();
    }

    private Stream<GraphQLFieldDefinition> getQueryFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        Either<Type, ParsedSchema> valueSchema = engine.getValueSchema(topic);

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory =
            new GraphQLQueryFactory(engine, topic, keySchema, valueSchema, objectType);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(new GraphQLList(objectType))
            .dataFetcher(new EntityFetcher(queryFactory))
            .argument(getWhereArgument(topic, keySchema, valueSchema))
            .argument(getLimitArgument())
            .argument(getOffsetArgument())
            .argument(getOrderByArgument(topic, keySchema, valueSchema))
            .build());
    }

    private boolean isObject(ParsedSchema schema) {
        switch (schema.schemaType()) {
            case "AVRO":
                return ((org.apache.avro.Schema) schema.rawSchema()).getType() == Schema.Type.RECORD;
            case "JSON":
                return schema.rawSchema() instanceof ObjectSchema;
            case "PROTOBUF":
            default:
                return true;
        }
    }

    private GraphQLArgument getWhereArgument(String topic,
                                             Either<Type, ParsedSchema> keySchema,
                                             Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.QUERY_WHERE, false);
        GraphQLInputType keyObject = Scalars.GraphQLString;
        /*
        GraphQLInputObjectType keyObject =
            (GraphQLInputObjectType) avroBuilder.createInputType(
                ctx, ((AvroSchema) keySchema.get()).rawSchema());

         */
        GraphQLInputType valueObject = avroBuilder.createInputType(
            ctx, ((AvroSchema) valueSchema.get()).rawSchema());

        GraphQLInputObjectType whereInputObject = getWhereObject(ctx, topic, keyObject, valueObject);

        return GraphQLArgument.newArgument()
            .name(WHERE_PARAM_NAME)
            .description("Where logical specification")
            .type(whereInputObject)
            .build();
    }

    private GraphQLInputObjectType getWhereObject(SchemaContext ctx,
                                                  String topic,
                                                  GraphQLInputType keyObject,
                                                  GraphQLInputType valueObject) {
        String name = topic + "_record_criteria";
        GraphQLInputObjectType type = (GraphQLInputObjectType) typeCache.get(name);
        if (type != null) {
            return type;
        }

        type = GraphQLInputObjectType.newInputObject()
                .name(name)
                .description(topic + " record criteria")
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(KEY_ATTR_NAME)
                    .description("Kafka record key")
                    .type(keyObject)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(VALUE_ATTR_NAME)
                    .description("Kafka record value")
                    .type(valueObject)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(TOPIC_ATTR_NAME)
                    .description("Kafka topic")
                    .type(createInputFieldOp(ctx, name, TOPIC_ATTR_NAME, Scalars.GraphQLString))
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(PARTITION_ATTR_NAME)
                    .description("Kafka partition")
                    .type(createInputFieldOp(ctx, name, PARTITION_ATTR_NAME, Scalars.GraphQLInt))
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(OFFSET_ATTR_NAME)
                    .description("Kafka record offset")
                    .type(createInputFieldOp(ctx, name, OFFSET_ATTR_NAME, ExtendedScalars.GraphQLLong))
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name(TIMESTAMP_ATTR_NAME)
                    .description("Kafka record timestamp")
                    .type(createInputFieldOp(ctx, name, TIMESTAMP_ATTR_NAME, ExtendedScalars.GraphQLLong))
                    .build())
                .build();
        typeCache.put(name, type);
        return type;
    }

    public static GraphQLInputType createInputFieldOp(SchemaContext ctx,
                                                      String typeName,
                                                      String fieldName,
                                                      GraphQLInputType fieldType) {
        String name = ctx.qualify(typeName + "_" + fieldName);
        fieldType = GraphQLInputObjectType.newInputObject()
            .name(name)
            .description("Criteria expression specification of "
                + fieldName + " attribute in entity " + typeName)
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
        return fieldType;
    }

    private GraphQLArgument getLimitArgument() {
        return GraphQLArgument.newArgument()
            .name(LIMIT_PARAM_NAME)
            .description("Limit the result set to the given number")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLArgument getOffsetArgument() {
        return GraphQLArgument.newArgument()
            .name(OFFSET_PARAM_NAME)
            .description("Start offset for the result set")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLArgument getOrderByArgument(String topic,
                                               Either<Type, ParsedSchema> keySchema,
                                               Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.QUERY_ORDER_BY, false);
        GraphQLInputType keyObject = Scalars.GraphQLString;
        /*
        GraphQLInputType keyObject = avroBuilder.createInputType(
            ctx, ((AvroSchema) keySchema.get()).rawSchema());

         */
        GraphQLInputType valueObject = avroBuilder.createInputType(
            ctx, ((AvroSchema) valueSchema.get()).rawSchema());

        String name = topic + "_record_sort";
        GraphQLInputObjectType orderByInputObject = GraphQLInputObjectType.newInputObject()
            .name(name)
            .description(topic + " record sort")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(KEY_ATTR_NAME)
                .description("Kafka record key")
                .type(keyObject)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(VALUE_ATTR_NAME)
                .description("Kafka record value")
                .type(valueObject)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(TOPIC_ATTR_NAME)
                .description("Kafka topic")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(PARTITION_ATTR_NAME)
                .description("Kafka partition")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(OFFSET_ATTR_NAME)
                .description("Kafka record offset")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(TIMESTAMP_ATTR_NAME)
                .description("Kafka record timestamp")
                .type(orderByEnum)
                .build())
            .build();

        return GraphQLArgument.newArgument()
            .name(ORDER_BY_PARAM_NAME)
            .description("Order by specification")
            .type(new GraphQLList(orderByInputObject))
            .build();

    }

    private GraphQLObjectType getObjectType(String topic,
                                            Either<Type, ParsedSchema> keySchema,
                                            Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.OUTPUT, false);

        GraphQLOutputType keyObject = Scalars.GraphQLString;
            /*
            (GraphQLObjectType) avroBuilder.createOutputType(
                ctx, ((AvroSchema) keySchema.get()).rawSchema());

             */
        GraphQLOutputType valueObject = avroBuilder.createOutputType(
            ctx, ((AvroSchema) valueSchema.get()).rawSchema());

        String name = topic;
        GraphQLObjectType type = (GraphQLObjectType) typeCache.get(name);
        if (type != null) {
            return type;
        }

        GraphQLObjectType objectType = GraphQLObjectType.newObject()
            .name(name)
            .description(topic)
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(KEY_ATTR_NAME)
                .description("Kafka record key")
                .type(keyObject)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(VALUE_ATTR_NAME)
                .description("Kafka record value")
                .type(valueObject)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(TOPIC_ATTR_NAME)
                .description("Kafka topic")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(PARTITION_ATTR_NAME)
                .description("Kafka partition")
                .type(Scalars.GraphQLInt)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(OFFSET_ATTR_NAME)
                .description("Kafka record offset")
                .type(ExtendedScalars.GraphQLLong)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(TIMESTAMP_ATTR_NAME)
                .description("Kafka record timestamp")
                .type(ExtendedScalars.GraphQLLong)
                .build())
            .build();

        typeCache.put(name, objectType);

        return objectType;
    }

    private GraphQLObjectType getMutationType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder mutationType = GraphQLObjectType.newObject()
            .name(MUTATION_ROOT)
            .description("Mutations for Kafka topics");
        mutationType.fields(topics.stream()
            .flatMap(t -> getMutationFieldDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return mutationType.build();
    }

    private Stream<GraphQLFieldDefinition> getMutationFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        Either<Type, ParsedSchema> valueSchema = engine.getValueSchema(topic);

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(objectType)
            .dataFetcher(new MutationFetcher(engine, topic, keySchema, valueSchema))
            .argument(getKeyArgument(topic, keySchema, valueSchema))
            .argument(getValueArgument(topic, keySchema, valueSchema))
            .build());
    }

    private GraphQLArgument getKeyArgument(String topic,
                                           Either<Type, ParsedSchema> keySchema,
                                           Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.MUTATION, false);
        GraphQLInputType keyObject = Scalars.GraphQLString;
        /*
        GraphQLInputType keyObject = avroBuilder.createInputType(
            ctx, ((AvroSchema) keySchema.get()).rawSchema());

         */

        return GraphQLArgument.newArgument()
            .name(KEY_ATTR_NAME)
            .description("Key specification")
            .type(keyObject)
            .build();
    }

    private GraphQLArgument getValueArgument(String topic,
                                             Either<Type, ParsedSchema> keySchema,
                                             Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.MUTATION, false);
        GraphQLInputType valueObject = avroBuilder.createInputType(
            ctx, ((AvroSchema) valueSchema.get()).rawSchema());

        return GraphQLArgument.newArgument()
            .name(VALUE_ATTR_NAME)
            .description("Value specification")
            .type(valueObject)
            .build();
    }

    private GraphQLObjectType getSubscriptionType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder subType = GraphQLObjectType.newObject()
            .name(SUBSCRIPTION_ROOT)
            .description("Subscriptions for Kafka topics");
        subType.fields(topics.stream()
            .flatMap(t -> getQueryFieldStreamDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return subType.build();
    }

    private Stream<GraphQLFieldDefinition> getQueryFieldStreamDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        Either<Type, ParsedSchema> valueSchema = engine.getValueSchema(topic);

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory =
            new GraphQLQueryFactory(engine, topic, keySchema, valueSchema, objectType);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(objectType)
            .dataFetcher(new SubscriptionFetcher(engine, topic,
                keySchema, valueSchema, queryFactory))
            .argument(getWhereArgument(topic, keySchema, valueSchema))
            .build());
    }
}
